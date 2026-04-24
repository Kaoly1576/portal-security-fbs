// ================== ACCESS DASHBOARD ==================

app.get("/access", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "access.html"));
});

const ACCESS_SPREADSHEET_ID = "11b7_2P62T1c1h1gnfYWYUQW2zQSNjteQTBn9jmW9QjU";
const ACCESS_RANGE = "'Solicitações'!A1:AE200000";

const ACCESS_COLUMNS = {
  data: "Data",
  estado: "Estado",
  cidade: "Cidade",
  operacao: "Operação",
  tipoLiberacao: "Tipo de Liberação",
  veiculo: "Vai acessar com veículo?",
  dataLiberacao: "Data de liberação",
  status: "Status",
  nome: "Nome completo:",
  cpf: "CPF",
  empresa: "Empresa",
  placa: "Placa",
};

let accessCache = null;
let accessCacheTime = 0;
const ACCESS_CACHE_TTL = 5 * 60 * 1000; // 5 min

function accessNormalize(value) {
  return String(value || "").trim();
}

function accessNormalizeLower(value) {
  return accessNormalize(value).toLowerCase();
}

function parseDateBRAccess(value) {
  const str = accessNormalize(value);
  if (!str) return null;

  if (/^\d{2}\/\d{2}\/\d{4}/.test(str)) {
    const [datePart] = str.split(" ");
    const [day, month, year] = datePart.split("/").map(Number);
    return new Date(year, month - 1, day);
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [year, month, day] = str.split("-").map(Number);
    return new Date(year, month - 1, day);
  }

  return null;
}

function formatInputDate(date) {
  if (!date) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function isStatusAprovado(value) {
  const v = accessNormalizeLower(value);
  return v === "aprovado" || v === "aprovada";
}

function isStatusReprovado(value) {
  const v = accessNormalizeLower(value);
  return v === "reprovado" || v === "reprovada";
}

function isComVeiculo(value) {
  return accessNormalizeLower(value) === "sim";
}

function isSemVeiculo(value) {
  const v = accessNormalizeLower(value);
  return v === "não" || v === "nao";
}

async function buscarPlanilhaAccessRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: ACCESS_SPREADSHEET_ID,
    range: ACCESS_RANGE,
  });

  return response.data.values || [];
}

async function buscarPlanilhaAccessComCache() {
  const now = Date.now();

  if (accessCache && (now - accessCacheTime) < ACCESS_CACHE_TTL) {
    return accessCache;
  }

  const dados = await buscarPlanilhaAccessRaw();

  if (!dados || dados.length < 2) {
    accessCache = [];
    accessCacheTime = now;
    return accessCache;
  }

  const cabecalho = dados[0] || [];
  const linhas = dados.slice(1);

  const objetos = linhas.map((linha) => {
    const obj = {};
    cabecalho.forEach((col, i) => {
      obj[col] = linha[i] ?? "";
    });

    obj._dataSolicitacao = parseDateBRAccess(obj[ACCESS_COLUMNS.data]);
    obj._dataLiberacao = parseDateBRAccess(obj[ACCESS_COLUMNS.dataLiberacao]);

    return obj;
  });

  accessCache = objetos;
  accessCacheTime = now;

  console.log("ACCESS cache atualizado:", objetos.length);

  return accessCache;
}

function getUniqueValuesAccess(data, column) {
  return [...new Set(data.map((row) => accessNormalize(row[column])).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "pt-BR"));
}

function groupCountAccess(data, column) {
  const map = {};
  data.forEach((row) => {
    const key = accessNormalize(row[column]) || "Sem valor";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

function filtrarAccessData(data, query) {
  const dataInicio = accessNormalize(query.dataInicio);
  const dataFim = accessNormalize(query.dataFim);
  const liberacaoInicio = accessNormalize(query.liberacaoInicio);
  const liberacaoFim = accessNormalize(query.liberacaoFim);
 const estados = accessParseMultiValue(query.estado);
const cidades = accessParseMultiValue(query.cidade);
const operacoes = accessParseMultiValue(query.operacao);
const tiposLiberacao = accessParseMultiValue(query.tipoLiberacao);
const veiculos = accessParseMultiValue(query.veiculo);
const statusList = accessParseMultiValue(query.status);
  const busca = accessNormalizeLower(query.busca);

  return data.filter((row) => {
    const rowData = row._dataSolicitacao;
    const rowLiberacao = row._dataLiberacao;

    const okDataInicio = !dataInicio || (rowData && formatInputDate(rowData) >= dataInicio);
    const okDataFim = !dataFim || (rowData && formatInputDate(rowData) <= dataFim);

    const okLiberacaoInicio =
      !liberacaoInicio || (rowLiberacao && formatInputDate(rowLiberacao) >= liberacaoInicio);
    const okLiberacaoFim =
      !liberacaoFim || (rowLiberacao && formatInputDate(rowLiberacao) <= liberacaoFim);

    const okEstado = accessMatchesMulti(row[ACCESS_COLUMNS.estado], estados);
const okCidade = accessMatchesMulti(row[ACCESS_COLUMNS.cidade], cidades);
const okOperacao = accessMatchesMulti(row[ACCESS_COLUMNS.operacao], operacoes);
const okTipo = accessMatchesMulti(row[ACCESS_COLUMNS.tipoLiberacao], tiposLiberacao);
const okVeiculo = accessMatchesMulti(row[ACCESS_COLUMNS.veiculo], veiculos);
const okStatus = accessMatchesMulti(row[ACCESS_COLUMNS.status], statusList);

    const searchable = [
      row[ACCESS_COLUMNS.nome],
      row[ACCESS_COLUMNS.cpf],
      row[ACCESS_COLUMNS.empresa],
      row[ACCESS_COLUMNS.placa],
      row[ACCESS_COLUMNS.cidade],
      row[ACCESS_COLUMNS.operacao],
      row[ACCESS_COLUMNS.tipoLiberacao],
      row[ACCESS_COLUMNS.status],
    ]
      .map(accessNormalizeLower)
      .join(" ");

    const okBusca = !busca || searchable.includes(busca);

    return (
      okDataInicio &&
      okDataFim &&
      okLiberacaoInicio &&
      okLiberacaoFim &&
      okEstado &&
      okCidade &&
      okOperacao &&
      okTipo &&
      okVeiculo &&
      okStatus &&
      okBusca
    );
  });
}

app.get("/api/access-debug", requireAuth, async (req, res) => {
  try {
    const sheets = await conectarSheets();

    const meta = await sheets.spreadsheets.get({
      spreadsheetId: ACCESS_SPREADSHEET_ID,
    });

    const abas = (meta.data.sheets || []).map(
      (s) => s.properties?.title || "SEM_NOME"
    );

    let values = [];
    let erroLeitura = null;

    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: ACCESS_SPREADSHEET_ID,
        range: ACCESS_RANGE,
      });

      values = response.data.values || [];
    } catch (err) {
      erroLeitura = {
        message: err.message,
        details: err.response?.data || null,
      };
    }

    return res.json({
      ok: true,
      spreadsheetId: ACCESS_SPREADSHEET_ID,
      abas,
      totalLinhasTeste: values.length,
      primeiraLinha: values[0] || null,
      erroLeitura,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

app.get("/api/access-filtros", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaAccessComCache();

    return res.json({
      estados: getUniqueValuesAccess(dados, ACCESS_COLUMNS.estado),
      cidades: getUniqueValuesAccess(dados, ACCESS_COLUMNS.cidade),
      operacoes: getUniqueValuesAccess(dados, ACCESS_COLUMNS.operacao),
      tiposLiberacao: getUniqueValuesAccess(dados, ACCESS_COLUMNS.tipoLiberacao),
      veiculos: getUniqueValuesAccess(dados, ACCESS_COLUMNS.veiculo),
      status: getUniqueValuesAccess(dados, ACCESS_COLUMNS.status),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

app.get("/api/access-resumo", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaAccessComCache();
    const filtrados = filtrarAccessData(dados, req.query);

    const total = filtrados.length;
    const aprovados = filtrados.filter((r) => isStatusAprovado(r[ACCESS_COLUMNS.status])).length;
    const reprovados = filtrados.filter((r) => isStatusReprovado(r[ACCESS_COLUMNS.status])).length;
    const comVeiculo = filtrados.filter((r) => isComVeiculo(r[ACCESS_COLUMNS.veiculo])).length;
    const semVeiculo = filtrados.filter((r) => isSemVeiculo(r[ACCESS_COLUMNS.veiculo])).length;
    const percentualAprovados = total ? (aprovados / total) * 100 : 0;

    return res.json({
      total,
      aprovados,
      reprovados,
      comVeiculo,
      semVeiculo,
      percentualAprovados,
      ultimaAtualizacao: getHoraAtualizacaoBR(),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

app.get("/api/access-graficos", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaAccessComCache();
    const filtrados = filtrarAccessData(dados, req.query);

    // Mantidos para compatibilidade, caso o front ainda consuma alguma dessas chaves
    const statusMap = groupCountAccess(filtrados, ACCESS_COLUMNS.status);
    const estadoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.estado);
    const tipoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.tipoLiberacao);
    const operacaoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.operacao);

    // Agora porDia retorna SOMENTE APROVADOS e sempre de 1 a 31
    const dayMap = {};
    filtrados
      .filter((row) => isStatusAprovado(row[ACCESS_COLUMNS.status]))
      .forEach((row) => {
        if (!row._dataSolicitacao) return;
        const day = String(row._dataSolicitacao.getDate()).padStart(2, "0");
        dayMap[day] = (dayMap[day] || 0) + 1;
      });

    const diaLabels = Array.from({ length: 31 }, (_, i) =>
      String(i + 1).padStart(2, "0")
    );

    const cidadeMap = {};
    const operacaoResumoMap = {};

    filtrados.forEach((row) => {
      const cidade = accessNormalize(row[ACCESS_COLUMNS.cidade]) || "Sem cidade";
      const operacao = accessNormalize(row[ACCESS_COLUMNS.operacao]) || "Sem operação";
      const aprovado = isStatusAprovado(row[ACCESS_COLUMNS.status]);
      const reprovado = isStatusReprovado(row[ACCESS_COLUMNS.status]);
      const comVeic = isComVeiculo(row[ACCESS_COLUMNS.veiculo]);

      if (!cidadeMap[cidade]) {
        cidadeMap[cidade] = { total: 0, aprovados: 0, reprovados: 0 };
      }

      if (!operacaoResumoMap[operacao]) {
        operacaoResumoMap[operacao] = { total: 0, aprovados: 0, comVeiculo: 0 };
      }

      cidadeMap[cidade].total += 1;
      operacaoResumoMap[operacao].total += 1;

      if (aprovado) {
        cidadeMap[cidade].aprovados += 1;
        operacaoResumoMap[operacao].aprovados += 1;
      }

      if (reprovado) {
        cidadeMap[cidade].reprovados += 1;
      }

      if (comVeic) {
        operacaoResumoMap[operacao].comVeiculo += 1;
      }
    });

    return res.json({
      // mantidas para compatibilidade
      status: statusMap,
      estado: operacaoMap, // front novo pode usar essa chave como "Acessos por Operação"
      tipoLiberacao: tipoMap,
      operacao: operacaoMap,

      porDia: {
        labels: diaLabels,
        values: diaLabels.map((label) => dayMap[label] || 0),
      },

      tabelaCidade: Object.entries(cidadeMap)
        .sort((a, b) => b[1].total - a[1].total)
        .slice(0, 50)
        .map(([cidade, info]) => ({
          cidade,
          total: info.total,
          aprovados: info.aprovados,
          reprovados: info.reprovados,
        })),

      tabelaOperacao: Object.entries(operacaoResumoMap)
        .sort((a, b) => b[1].total - a[1].total)
        .slice(0, 50)
        .map(([operacao, info]) => ({
          operacao,
          total: info.total,
          aprovados: info.aprovados,
          comVeiculo: info.comVeiculo,
        })),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

app.get("/api/access-detalhes", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaAccessComCache();
    const filtrados = filtrarAccessData(dados, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const start = (page - 1) * limit;
    const end = start + limit;

    const rows = filtrados.slice(start, end).map((row) => ({
      ID: row["ID"] || "",
      [ACCESS_COLUMNS.data]: row[ACCESS_COLUMNS.data] || "",
      [ACCESS_COLUMNS.dataLiberacao]: row[ACCESS_COLUMNS.dataLiberacao] || "",
      [ACCESS_COLUMNS.estado]: row[ACCESS_COLUMNS.estado] || "",
      [ACCESS_COLUMNS.cidade]: row[ACCESS_COLUMNS.cidade] || "",
      [ACCESS_COLUMNS.operacao]: row[ACCESS_COLUMNS.operacao] || "",
      [ACCESS_COLUMNS.tipoLiberacao]: row[ACCESS_COLUMNS.tipoLiberacao] || "",
      [ACCESS_COLUMNS.nome]: row[ACCESS_COLUMNS.nome] || "",
      [ACCESS_COLUMNS.empresa]: row[ACCESS_COLUMNS.empresa] || "",
      [ACCESS_COLUMNS.veiculo]: row[ACCESS_COLUMNS.veiculo] || "",
      [ACCESS_COLUMNS.placa]: row[ACCESS_COLUMNS.placa] || "",
      [ACCESS_COLUMNS.status]: row[ACCESS_COLUMNS.status] || "",
    }));

    return res.json({
      total: filtrados.length,
      page,
      limit,
      totalPages: Math.ceil(filtrados.length / limit),
      rows,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

function accessParseMultiValue(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map(v => accessNormalize(v))
    .filter(Boolean);
}

function accessMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(accessNormalize(fieldValue));
}
