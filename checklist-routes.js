const fs = require("fs");
const path = require("path");

function createChecklistRoutes(options = {}) {
  const CONFIG = {
    sheetId: options.sheetId || "1vUv0AJ_bASBkkzxUWOyheJSASb4VDsaRKAl40EzRKsk",
    sheetName: options.sheetName || "Respostas",
    fallbackCsvPath: options.fallbackCsvPath || process.env.CHECKLIST_CSV_PATH || "",
    portalUrl: options.portalUrl || "/portal",
    cacheMs: Number(options.cacheMs || 5 * 60 * 1000),
    pageSizeDefault: Number(options.pageSizeDefault || 20),
    htmlPath:
      options.htmlPath || path.join(__dirname, "public", "dashboard-checklist.html"),
    logoPath: options.logoPath || path.join(__dirname, "public", "logo.png"),
    requireAuth: options.requireAuth || null,
    conectarSheets: options.conectarSheets || null,
  };

  let cache = {
    expiresAt: 0,
    rows: [],
    source: "",
  };

  function decodePossiblyBrokenText(value) {
    let text = String(value ?? "").replace(/^\uFEFF/, "").trim();
    if (!text) return "";
    if (/[ÃÂâ€™â€œâ€â€“]/.test(text)) {
      try {
        const repaired = Buffer.from(text, "latin1").toString("utf8");
        if (repaired && !/\uFFFD/.test(repaired)) {
          text = repaired;
        }
      } catch (_error) {}
    }
    return text.replace(/\s+/g, " ").trim();
  }

  function parseCsv(text) {
    const rows = [];
    let current = "";
    let row = [];
    let inQuotes = false;

    for (let i = 0; i < text.length; i += 1) {
      const char = text[i];
      const next = text[i + 1];

      if (char === "\"") {
        if (inQuotes && next === "\"") {
          current += "\"";
          i += 1;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }

      if (char === "," && !inQuotes) {
        row.push(current);
        current = "";
        continue;
      }

      if ((char === "\n" || char === "\r") && !inQuotes) {
        if (char === "\r" && next === "\n") i += 1;
        row.push(current);
        rows.push(row);
        row = [];
        current = "";
        continue;
      }

      current += char;
    }

    if (current.length || row.length) {
      row.push(current);
      rows.push(row);
    }

    const headers = (rows.shift() || []).map((header) => decodePossiblyBrokenText(header));

    return rows
      .filter((items) => items.some((item) => String(item || "").trim() !== ""))
      .map((items) => {
        const entry = {};
        headers.forEach((header, index) => {
          entry[header] = decodePossiblyBrokenText(items[index] || "");
        });
        return entry;
      });
  }

  function parseDate(value) {
    const text = decodePossiblyBrokenText(value);
    if (!text) return null;

    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
      const date = new Date(`${text}T00:00:00`);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    if (/^\d{4}-\d{2}-\d{2}T/.test(text)) {
      const iso = new Date(text);
      return Number.isNaN(iso.getTime()) ? null : iso;
    }

    const slash = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (slash) {
      const month = Number(slash[1]) - 1;
      const day = Number(slash[2]);
      const year = Number(slash[3]);
      const date = new Date(year, month, day);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    const fallback = new Date(text);
    return Number.isNaN(fallback.getTime()) ? null : fallback;
  }

  function formatDate(date) {
    if (!date || Number.isNaN(date.getTime())) return "";
    return date.toLocaleDateString("pt-BR");
  }

  function formatDateTime(date) {
    if (!date || Number.isNaN(date.getTime())) return "";
    return date.toLocaleTimeString("pt-BR", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "America/Sao_Paulo",
    });
  }

  function startOfDay(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
  }

  function endOfDay(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
  }

  function addDays(date, days) {
    const next = new Date(date);
    next.setDate(next.getDate() + days);
    return next;
  }

  function addMonths(date, months) {
    return new Date(date.getFullYear(), date.getMonth() + months, date.getDate());
  }

  function getWeekBounds(date) {
    const current = startOfDay(date);
    const day = current.getDay();
    const mondayOffset = day === 0 ? -6 : 1 - day;
    const start = addDays(current, mondayOffset);
    const end = endOfDay(addDays(start, 6));
    return { start, end };
  }

  function getPeriodBounds(date, periodType, offset = 0) {
    const ref = startOfDay(date);

    if (periodType === "dia") {
      const current = addDays(ref, offset);
      return { start: startOfDay(current), end: endOfDay(current) };
    }

    if (periodType === "semana") {
      return getWeekBounds(addDays(ref, offset * 7));
    }

    if (periodType === "ano") {
      const year = ref.getFullYear() + offset;
      return {
        start: new Date(year, 0, 1),
        end: new Date(year, 11, 31, 23, 59, 59, 999),
      };
    }

    const base = addMonths(ref, offset);
    return {
      start: new Date(base.getFullYear(), base.getMonth(), 1),
      end: new Date(base.getFullYear(), base.getMonth() + 1, 0, 23, 59, 59, 999),
    };
  }

  function getComparisonLabel(periodType) {
    if (periodType === "dia") return "dia anterior";
    if (periodType === "semana") return "semana anterior";
    if (periodType === "ano") return "ano anterior";
    return "mês anterior";
  }

  function splitMultiValue(value) {
    return decodePossiblyBrokenText(value)
      .split("|")
      .map((item) => decodePossiblyBrokenText(item))
      .filter(Boolean);
  }

  function uniqueSorted(values) {
    return [...new Set(values.filter(Boolean))].sort((a, b) =>
      a.localeCompare(b, "pt-BR", { sensitivity: "base" })
    );
  }

  function groupTop(rows, field, limit = 8) {
    const counts = new Map();

    rows.forEach((row) => {
      const key = decodePossiblyBrokenText(row[field]) || "Não informado";
      counts.set(key, (counts.get(key) || 0) + 1);
    });

    const sorted = [...counts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "pt-BR", { sensitivity: "base" }))
      .slice(0, limit);

    return {
      labels: sorted.map(([label]) => label),
      valores: sorted.map(([, value]) => value),
    };
  }

  function buildDaySeries(rows) {
    const labels = Array.from({ length: 31 }, (_, index) => String(index + 1).padStart(2, "0"));
    const counts = Array(31).fill(0);

    rows.forEach((row) => {
      if (!row.dataChecklistDate) return;
      const day = row.dataChecklistDate.getDate();
      if (day >= 1 && day <= 31) counts[day - 1] += 1;
    });

    return { labels, valores: counts };
  }

  function normalizeRow(row) {
    const dataChecklistDate = parseDate(row.data_checklist);
    const dataLimiteDate = parseDate(row.data_limite);
    const atualizacaoDate = parseDate(row.data_atualizacao);
    const quantidade = decodePossiblyBrokenText(row.quantidade);
    const resposta = decodePossiblyBrokenText(row.resposta);

    return {
      respostaId: decodePossiblyBrokenText(row.resposta_id),
      registroId: decodePossiblyBrokenText(row.registro_id),
      dataChecklistDate,
      dataChecklist: formatDate(dataChecklistDate),
      elaboradoPor: decodePossiblyBrokenText(row.elaborado_por),
      funcao: decodePossiblyBrokenText(row.funcao),
      unidade: decodePossiblyBrokenText(row.unidade),
      estado: decodePossiblyBrokenText(row.estado),
      cidade: decodePossiblyBrokenText(row.cidade),
      topico: decodePossiblyBrokenText(row.topico),
      perguntaId: decodePossiblyBrokenText(row.pergunta_id),
      perguntaTexto: decodePossiblyBrokenText(row.pergunta_texto),
      peso: decodePossiblyBrokenText(row.peso),
      tipoPergunta: decodePossiblyBrokenText(row.tipo_pergunta),
      resposta: resposta || (quantidade ? `Quantidade: ${quantidade}` : "Sem resposta"),
      geraNc: decodePossiblyBrokenText(row.gera_nc) || "NAO",
      responsavelTratativa: decodePossiblyBrokenText(row.responsavel_tratativa),
      areaResponsavel: decodePossiblyBrokenText(row.area_responsavel),
      dataLimiteDate,
      dataLimite: formatDate(dataLimiteDate),
      situacaoPrazo: decodePossiblyBrokenText(row.situacao_prazo),
      pontuacao: decodePossiblyBrokenText(row.pontuacao),
      atualizacaoDate,
    };
  }

  async function fetchSheetCsvWithGoogleApi() {
    if (!CONFIG.conectarSheets) {
      throw new Error("conectarSheets não informado.");
    }

    const sheets = await CONFIG.conectarSheets();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.sheetId,
      range: `'${CONFIG.sheetName}'!A1:Z200000`,
    });

    const values = response.data.values || [];
    if (!values.length) return [];

    const headers = values[0].map((header) => decodePossiblyBrokenText(header));
    return values.slice(1).map((items) => {
      const entry = {};
      headers.forEach((header, index) => {
        entry[header] = decodePossiblyBrokenText(items[index] || "");
      });
      return entry;
    });
  }

  async function loadRows() {
    const now = Date.now();
    if (cache.rows.length && cache.expiresAt > now) return cache.rows;

    let rows = [];
    let source = "";

    try {
      rows = await fetchSheetCsvWithGoogleApi();
      source = "google-sheets-api";
    } catch (_error) {
      if (!CONFIG.fallbackCsvPath) throw _error;
      const text = fs.readFileSync(CONFIG.fallbackCsvPath, "utf8");
      rows = parseCsv(text);
      source = "csv-local";
    }

    cache = {
      expiresAt: now + CONFIG.cacheMs,
      rows: rows.map(normalizeRow),
      source,
    };

    return cache.rows;
  }

  function getFilters(query) {
    return {
      periodoTipo: decodePossiblyBrokenText(query.periodoTipo) || "mes",
      dataRef: parseDate(query.dataRef) || new Date(),
      unidade: splitMultiValue(query.unidade),
      estado: splitMultiValue(query.estado),
      cidade: splitMultiValue(query.cidade),
      topico: splitMultiValue(query.topico),
      elaboradoPor: splitMultiValue(query.elaboradoPor),
      resposta: splitMultiValue(query.resposta),
      geraNc: splitMultiValue(query.geraNc),
      areaResponsavel: splitMultiValue(query.areaResponsavel),
      situacaoPrazo: splitMultiValue(query.situacaoPrazo),
      busca: decodePossiblyBrokenText(query.busca).toLocaleLowerCase("pt-BR"),
      page: Math.max(Number(query.page) || 1, 1),
      limit: Math.max(Number(query.limit) || CONFIG.pageSizeDefault, 1),
    };
  }

  function applyFilters(rows, filters) {
    return rows.filter((row) => {
      if (filters.unidade.length && !filters.unidade.includes(row.unidade)) return false;
      if (filters.estado.length && !filters.estado.includes(row.estado)) return false;
      if (filters.cidade.length && !filters.cidade.includes(row.cidade)) return false;
      if (filters.topico.length && !filters.topico.includes(row.topico)) return false;
      if (filters.elaboradoPor.length && !filters.elaboradoPor.includes(row.elaboradoPor)) return false;
      if (filters.resposta.length && !filters.resposta.includes(row.resposta)) return false;
      if (filters.geraNc.length && !filters.geraNc.includes(row.geraNc)) return false;
      if (filters.areaResponsavel.length && !filters.areaResponsavel.includes(row.areaResponsavel)) return false;
      if (filters.situacaoPrazo.length && !filters.situacaoPrazo.includes(row.situacaoPrazo)) return false;

      if (filters.busca) {
        const haystack = [
          row.registroId,
          row.unidade,
          row.estado,
          row.cidade,
          row.topico,
          row.perguntaTexto,
          row.resposta,
          row.elaboradoPor,
          row.responsavelTratativa,
          row.areaResponsavel,
          row.situacaoPrazo,
        ]
          .join(" ")
          .toLocaleLowerCase("pt-BR");

        if (!haystack.includes(filters.busca)) return false;
      }

      return true;
    });
  }

  function filterByBounds(rows, bounds) {
    return rows.filter(
      (row) =>
        row.dataChecklistDate &&
        row.dataChecklistDate >= bounds.start &&
        row.dataChecklistDate <= bounds.end
    );
  }

  function getDataset(rows, filters) {
    const filtered = applyFilters(rows, filters);
    const currentBounds = getPeriodBounds(filters.dataRef, filters.periodoTipo, 0);
    const previousBounds = getPeriodBounds(filters.dataRef, filters.periodoTipo, -1);

    return {
      filtered,
      periodoAtual: filterByBounds(filtered, currentBounds),
      periodoAnterior: filterByBounds(filtered, previousBounds),
      currentBounds,
      previousBounds,
    };
  }

  function latestUpdate(rows) {
    const valid = rows
      .map((row) => row.atualizacaoDate)
      .filter((date) => date && !Number.isNaN(date.getTime()))
      .sort((a, b) => b - a);

    return valid[0] ? formatDateTime(valid[0]) : "";
  }

  function buildResumo(rows, filters) {
    const data = getDataset(rows, filters);
    const atual = data.periodoAtual.length;
    const anterior = data.periodoAnterior.length;
    const variacao = anterior === 0 ? (atual > 0 ? 100 : 0) : ((atual - anterior) / anterior) * 100;

    return {
      total: data.filtered.length,
      periodoAtual: atual,
      periodoAnterior: anterior,
      variacao,
      checklistsUnicos: new Set(data.periodoAtual.map((item) => item.registroId).filter(Boolean)).size,
      itensNc: data.periodoAtual.filter((item) => item.geraNc === "SIM").length,
      periodoAtualInicio: formatDate(data.currentBounds.start),
      periodoAtualFim: formatDate(data.currentBounds.end),
      periodoAnteriorInicio: formatDate(data.previousBounds.start),
      periodoAnteriorFim: formatDate(data.previousBounds.end),
      comparativoLabel: getComparisonLabel(filters.periodoTipo),
      ultimaAtualizacao: latestUpdate(rows),
    };
  }

  function buildGraficos(rows, filters) {
    const data = getDataset(rows, filters);

    return {
      porUnidade: groupTop(data.periodoAtual, "unidade"),
      porTopico: groupTop(data.periodoAtual, "topico"),
      porResposta: groupTop(data.periodoAtual, "resposta"),
      porDia: buildDaySeries(data.periodoAtual),
      comparativo: {
        atual: data.periodoAtual.length,
        anterior: data.periodoAnterior.length,
        label: getComparisonLabel(filters.periodoTipo),
      },
    };
  }

  function buildDetalhes(rows, filters) {
    const data = getDataset(rows, filters);

    const ordered = [...data.periodoAtual].sort((a, b) => {
      const dateA = a.dataChecklistDate ? a.dataChecklistDate.getTime() : 0;
      const dateB = b.dataChecklistDate ? b.dataChecklistDate.getTime() : 0;
      return dateB - dateA || a.unidade.localeCompare(b.unidade, "pt-BR", { sensitivity: "base" });
    });

    const totalPages = Math.max(Math.ceil(ordered.length / filters.limit), 1);
    const page = Math.min(filters.page, totalPages);
    const start = (page - 1) * filters.limit;

    return {
      page,
      totalPages,
      totalItems: ordered.length,
      items: ordered.slice(start, start + filters.limit),
    };
  }

  function buildFilterOptions(rows) {
    return {
      unidades: uniqueSorted(rows.map((row) => row.unidade)),
      estados: uniqueSorted(rows.map((row) => row.estado)),
      cidades: uniqueSorted(rows.map((row) => row.cidade)),
      topicos: uniqueSorted(rows.map((row) => row.topico)),
      elaboradoPor: uniqueSorted(rows.map((row) => row.elaboradoPor)),
      respostas: uniqueSorted(rows.map((row) => row.resposta)),
      geraNc: uniqueSorted(rows.map((row) => row.geraNc)),
      areaResponsavel: uniqueSorted(rows.map((row) => row.areaResponsavel)),
      situacaoPrazo: uniqueSorted(rows.map((row) => row.situacaoPrazo)),
    };
  }

  function wrap(handler) {
    return async (req, res) => {
      try {
        await handler(req, res);
      } catch (error) {
        console.error("Erro Checklist:", error);
        res.status(500).json({
          error: "Falha ao carregar dados do checklist.",
          detail: error.message,
        });
      }
    };
  }

  return {
    register(app) {
      const auth = CONFIG.requireAuth ? [CONFIG.requireAuth] : [];

      app.get("/dashboard-checklist", ...auth, (_req, res) => {
        return res.sendFile(CONFIG.htmlPath);
      });

      app.get("/images/logo.png", ...auth, (_req, res) => {
        if (fs.existsSync(CONFIG.logoPath)) {
          return res.sendFile(CONFIG.logoPath);
        }
        return res.status(404).send("logo.png não encontrado.");
      });

      app.get("/api/checklist/filtros", ...auth, wrap(async (_req, res) => {
        const rows = await loadRows();
        return res.json(buildFilterOptions(rows));
      }));

      app.get("/api/checklist/resumo", ...auth, wrap(async (req, res) => {
        const rows = await loadRows();
        return res.json(buildResumo(rows, getFilters(req.query)));
      }));

      app.get("/api/checklist/graficos", ...auth, wrap(async (req, res) => {
        const rows = await loadRows();
        return res.json(buildGraficos(rows, getFilters(req.query)));
      }));

      app.get("/api/checklist/detalhes", ...auth, wrap(async (req, res) => {
        const rows = await loadRows();
        return res.json(buildDetalhes(rows, getFilters(req.query)));
      }));
    },
  };
}

module.exports = { createChecklistRoutes };
