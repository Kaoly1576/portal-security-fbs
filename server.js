require("dotenv").config();

const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;
const { google } = require("googleapis");

const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const bcrypt = require("bcryptjs");
const session = require("express-session");
const path = require("path");

const app = express();

// ================== MIDDLEWARES ==================

app.set("trust proxy", 1);

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.use(
  session({
    name: "portal_security_sid",
    secret: process.env.SESSION_SECRET || "segredo_super_secreto",
    resave: false,
    saveUninitialized: false,
    proxy: true,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: true,
      maxAge: 1000 * 60 * 60 * 12
    },
  })
);

app.use(passport.initialize());
app.use(express.static(path.join(__dirname, "public")));

// ================== DATABASE ==================

const db = new sqlite3.Database("./database.db");

db.run(`
CREATE TABLE IF NOT EXISTS usuarios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nome TEXT,
  email TEXT UNIQUE,
  senha TEXT,
  perfil TEXT DEFAULT 'usuario',
  status TEXT DEFAULT 'pendente',
  cargo TEXT DEFAULT 'Não definido',
  nivel_acesso INTEGER DEFAULT 1,
  area TEXT DEFAULT 'Security',
  foto TEXT DEFAULT '',
  google_id TEXT DEFAULT '',
  permissoes TEXT DEFAULT '{}',
  criado_em DATETIME DEFAULT CURRENT_TIMESTAMP,
  atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP
)
`);

db.run(`
CREATE TABLE IF NOT EXISTS registros (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  valor_usd REAL DEFAULT 0,
  valor_brl REAL DEFAULT 0
)
`);

// ================== CRIAR APROVADOR ==================

const senhaHashAdmin = bcrypt.hashSync("Kaoly1576;", 10);

db.run(
  `
  INSERT OR IGNORE INTO usuarios (nome, email, senha, perfil, status)
  VALUES (?, ?, ?, ?, ?)
  `,
  [
    "Caique Nascimento",
    "caique.nascimento@shopee.com",
    senhaHashAdmin,
    "aprovador",
    "aprovado",
  ]
);

// ================== PASSPORT GOOGLE ==================

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_CALLBACK_URL =
  process.env.GOOGLE_CALLBACK_URL ||
  "http://localhost:3000/auth/google/callback";

const googleOAuthEnabled =
  Boolean(GOOGLE_CLIENT_ID) && Boolean(GOOGLE_CLIENT_SECRET);

if (googleOAuthEnabled) {
  passport.use(
    new GoogleStrategy(
      {
        clientID: GOOGLE_CLIENT_ID,
        clientSecret: GOOGLE_CLIENT_SECRET,
        callbackURL: GOOGLE_CALLBACK_URL,
      },
      (accessToken, refreshToken, profile, done) => {
        try {
          const email = profile?.emails?.[0]?.value || "";
          const nome = profile?.displayName || "Usuário";
          const foto = profile?.photos?.[0]?.value || "";

          if (!email.endsWith("@shopee.com")) {
            return done(null, false);
          }

          db.get("SELECT * FROM usuarios WHERE email = ?", [email], (err, user) => {
            if (err) return done(err);

            if (user) {
              return done(null, { ...user, foto });
            }

            db.run(
              `
              INSERT INTO usuarios (nome, email, senha, perfil, status)
              VALUES (?, ?, ?, ?, ?)
              `,
              [nome, email, "", "usuario", "pendente"],
              function (insErr) {
                if (insErr) return done(insErr);

                db.get(
                  "SELECT * FROM usuarios WHERE id = ?",
                  [this.lastID],
                  (selErr, newUser) => {
                    if (selErr) return done(selErr);
                    return done(null, { ...newUser, foto });
                  }
                );
              }
            );
          });
        } catch (e) {
          return done(e);
        }
      }
    )
  );

  console.log("Google OAuth habilitado.");
} else {
  console.warn("Google OAuth desabilitado: GOOGLE_CLIENT_ID ou GOOGLE_CLIENT_SECRET ausente.");
}

// ================== HELPERS ==================

function requireAuth(req, res, next) {
  console.log("AUTH CHECK:", {
    path: req.path,
    userId: req.session.userId || null,
    email: req.session.email || null,
    perfil: req.session.perfil || null
  });

  if (!req.session.userId) {
    return res.redirect("/login");
  }

  next();
}

function requireAprovador(req, res, next) {
  if (!req.session.userId) return res.redirect("/login");
  if (req.session.perfil !== "aprovador") {
    return res.status(403).send("Acesso negado.");
  }
  next();
}

async function conectarSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  return google.sheets({ version: "v4", auth });
}

async function conectarSheetsEdicao() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const client = await auth.getClient();

  return google.sheets({
    version: "v4",
    auth: client,
  });
}

function normalizeText(value = "") {
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();
}

function formatDateBR(dateInput) {
  if (!dateInput) return "";

  const value = String(dateInput).trim();

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(value)) {
    return value;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [year, month, day] = value.split("-");
    return `${day}/${month}/${year}`;
  }

  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";

  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();

  return `${day}/${month}/${year}`;
}

function toA1Column(colIndex) {
  let result = "";
  let n = colIndex + 1;

  while (n > 0) {
    const rem = (n - 1) % 26;
    result = String.fromCharCode(65 + rem) + result;
    n = Math.floor((n - 1) / 26);
  }

  return result;
}

function detectarLinhaDatas(rows) {
  const isDate = (value) =>
    /^\d{2}\/\d{2}\/\d{4}$/.test(String(value || "").trim());

  const dateRowIndex = rows.findIndex((row) => {
    const totalDatas = row.filter((cell) => isDate(cell)).length;
    return totalDatas >= 5;
  });

  if (dateRowIndex === -1) {
    throw new Error("Não encontrei a linha de datas na planilha.");
  }

  const dateRow = rows[dateRowIndex] || [];
  const firstDateColIndex = dateRow.findIndex((cell) => isDate(cell));

  if (firstDateColIndex === -1) {
    throw new Error("Não encontrei a primeira coluna de datas.");
  }

  const lateralDates = [];
  for (let i = firstDateColIndex; i < dateRow.length; i++) {
    const valor = String(dateRow[i] || "").trim();
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(valor)) {
      lateralDates.push({
        colIndex: i,
        date: valor,
      });
    }
  }

  return {
    dateRowIndex,
    firstDateColIndex,
    lateralDates,
  };
}

async function lerSheetChamada() {
  const sheets = await conectarSheetsEdicao();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "1OLFfhaPKAL92co8vaHqpy_OiOU9oFEEqOrpgqo3nMJ0",
    range: "'ABS AGENTES FBS'!A1:AZ",
  });

  const rows = response.data.values || [];

  if (!rows.length) {
    throw new Error("A planilha está vazia.");
  }

  const { dateRowIndex, firstDateColIndex, lateralDates } = detectarLinhaDatas(rows);

  const DATA_START_ROW = dateRowIndex + 3;

  const agents = rows
    .slice(DATA_START_ROW)
    .map((r, idx) => ({
      rowIndex: DATA_START_ROW + idx,
      genero: String(r[0] || "").trim(),
      colaborador: String(r[1] || "").trim(),
      hora: String(r[2] || "").trim(),
      escala: String(r[3] || "").trim(),
      dia_inicio: String(r[4] || "").trim(),
      re: String(r[5] || "").trim(),
      unidade: String(r[6] || "").trim(),
      lider: String(r[7] || "").trim(),
      admissao: String(r[8] || "").trim(),
      desligamento: String(r[9] || "").trim(),
      cargo: String(r[10] || "").trim(),
    }))
    .filter((a) => a.colaborador && a.re);

  return {
    rows,
    dateRowIndex,
    firstDateColIndex,
    lateralDates,
    agents,
  };
}

// ================== ROTAS PÚBLICAS ==================

app.get("/", (req, res) => {
  if (req.session.userId) return res.redirect("/portal");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.get("/login", (req, res) => {
  if (req.session.userId) return res.redirect("/portal");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

// ================== LOGIN GOOGLE ==================

app.get("/auth/google", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res
      .status(503)
      .send("Login Google não configurado no servidor.");
  }

  return passport.authenticate("google", {
    scope: ["openid", "profile", "email"],
    session: false,
  })(req, res, next);
});

app.get("/auth/google/callback", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.redirect("/login");
  }

  return passport.authenticate("google", {
    failureRedirect: "/login",
    session: false,
  })(req, res, next);
}, (req, res) => {
  if (!req.user) return res.redirect("/login");

  if (req.user.status !== "aprovado") {
    req.session.destroy(() => {
      return res.send("Seu acesso ainda está pendente de aprovação pelo Security.");
    });
    return;
  }

  req.session.userId = req.user.id;
  req.session.nome = req.user.nome;
  req.session.email = req.user.email;
  req.session.perfil = req.user.perfil;
  req.session.foto = req.user.foto || "";

  return res.redirect("/portal");
});

// ================== CADASTRO ==================

app.get("/cadastro", (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "cadastro.html"));
});

app.post("/cadastro", async (req, res) => {
  const { nome, email, senha } = req.body;

  if (!email || !senha || !nome) {
    return res.send("Preencha nome, e-mail e senha.");
  }

  if (!email.endsWith("@shopee.com")) {
    return res.send("Somente e-mails @shopee.com permitidos.");
  }

  const senhaHash = await bcrypt.hash(senha, 10);

  db.run(
    `INSERT INTO usuarios (nome, email, senha) VALUES (?, ?, ?)`,
    [nome, email, senhaHash],
    function (err) {
      if (err) return res.send("Usuário já existe ou erro no cadastro.");
      return res.send("Cadastro realizado! Aguarde aprovação do Security.");
    }
  );
});

// ================== LOGIN LOCAL ==================

app.post("/login", (req, res) => {
  const { email, senha } = req.body;

  if (!email || !senha) {
    return res.send("Informe e-mail e senha.");
  }

  db.get("SELECT * FROM usuarios WHERE email = ?", [email], async (err, user) => {
    if (err) return res.send("Erro no servidor.");
    if (!user) return res.send("Usuário não encontrado.");

    if (user.status !== "aprovado") {
      return res.send("Usuário ainda não aprovado pelo Security.");
    }

    const senhaValida = await bcrypt.compare(senha, user.senha || "");
    if (!senhaValida) return res.send("Senha incorreta.");

    req.session.userId = user.id;
    req.session.nome = user.nome;
    req.session.email = user.email;
    req.session.perfil = user.perfil;
    req.session.foto = user.foto || "";

    return res.redirect("/portal");
  });
});

// ================== LOGOUT ==================

app.get("/logout", (req, res) => {
  req.session.destroy(() => res.redirect("/login"));
});

// ================== ROTAS PROTEGIDAS ==================

app.get("/portal", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "portal.html"));
});

app.get("/dashboard", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "dashboard.html"));
});

app.get("/hc", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "hc.html"));
});

app.get("/agentes-fbs", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "agentes-fbs.html"));
});

app.get("/chamada-agentes", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "chamada-agentes.html"));
});

app.get("/api/me", requireAuth, (req, res) => {
  return res.json({
    id: req.session.userId,
    nome: req.session.nome || "",
    email: req.session.email || "",
    perfil: req.session.perfil || "",
    foto: req.session.foto || "",
  });
});

// ================== GOOGLE SHEETS DASHBOARD AV ==================

async function buscarPlanilha() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "1Fo62wRlUULj3lAQJYl8lcimGg-7YSIHpy7kZYr8rLlY",
    range: "AV!A:M",
  });

  return response.data.values || [];
}

app.get("/api/dados", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilha();
    const cabecalho = dados[0] || [];
    const linhas = dados.slice(1);

    const objetos = linhas.map((linha) => {
      const obj = {};
      cabecalho.forEach((col, i) => {
        obj[col] = linha[i] ?? "";
      });
      return obj;
    });

    return res.json(objetos);
  } catch (e) {
    console.log("Erro /api/dados:", e);
    return res.json([]);
  }
});

// ================== APROVAÇÕES ==================

app.get("/aprovacoes", requireAprovador, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "aprovacoes.html"));
});

app.get("/api/aprovacoes/pendentes", requireAprovador, (req, res) => {
  db.all(
    "SELECT id, nome, email, perfil, status FROM usuarios WHERE status = 'pendente' ORDER BY id DESC",
    (err, users) => {
      if (err) {
        console.log("Erro ao listar pendentes:", err);
        return res.status(500).json({ error: "Erro ao listar usuários pendentes." });
      }

      return res.json(users || []);
    }
  );
});

app.post("/api/aprovacoes/aprovar/:id", requireAprovador, (req, res) => {
  db.run(
    "UPDATE usuarios SET status = 'aprovado' WHERE id = ?",
    [req.params.id],
    function (err) {
      if (err) {
        console.log("Erro ao aprovar usuário:", err);
        return res.status(500).json({ error: "Erro ao aprovar usuário." });
      }

      return res.json({ success: true });
    }
  );
});

// ================== DADOS DASHBOARD LOCAL ==================

app.get("/dados-dashboard", requireAuth, (req, res) => {
  db.get(
    `
    SELECT 
      COUNT(*) as total,
      SUM(valor_usd) as usd,
      SUM(valor_brl) as brl
    FROM registros
    `,
    (err, row) => {
      if (err) return res.json({ total: 0, usd: 0, brl: 0 });
      if (!row) return res.json({ total: 0, usd: 0, brl: 0 });

      return res.json({
        total: row.total || 0,
        usd: row.usd || 0,
        brl: row.brl || 0,
      });
    }
  );
});

// ================== GOOGLE SHEETS HC ==================

async function buscarPlanilhaHC() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "1fD7pvbKwGwMHsww0IMQjkEqE4ohuBKv81MNoyV8tgbc",
    range: "'raw_hc'!A:AG",
  });

  return response.data.values || [];
}

app.get("/api/hc-dados", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaHC();

    if (!dados.length) {
      return res.json([]);
    }

    const cabecalho = dados[0] || [];
    const linhas = dados.slice(1);

    const objetos = linhas.map((linha) => {
      const obj = {};
      cabecalho.forEach((col, i) => {
        obj[col] = linha[i] ?? "";
      });
      return obj;
    });

    return res.json(objetos);
  } catch (e) {
    console.log("Erro HC nova base:", e);
    return res.json([]);
  }
});

// ================== HC AGENTES FBS ==================

// ================== HC AGENTES FBS ==================

const HC_AGENTES_FBS_SPREADSHEET_ID = "1XtP5ylCpA42aLE1EklytzHH2hUzik5JdY8meAg84Et4";
const HC_AGENTES_FBS_RANGE = "'CONTROLE DE FALTAS FBS'!A1:Z200000";

let hcAgentesCache = null;
let hcAgentesCacheTime = 0;
const HC_AGENTES_CACHE_TTL = 5 * 60 * 1000;

function hcNorm(value) {
  return String(value || "").trim();
}

function hcNormLower(value) {
  return hcNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function hcFindHeader(headers, possibilities) {
  for (const p of possibilities) {
    const exact = headers.find(h => hcNormLower(h) === hcNormLower(p));
    if (exact) return exact;
  }
  for (const p of possibilities) {
    const partial = headers.find(h => hcNormLower(h).includes(hcNormLower(p)));
    if (partial) return partial;
  }
  return null;
}

async function carregarHcAgentesFbsRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: HC_AGENTES_FBS_SPREADSHEET_ID,
    range: HC_AGENTES_FBS_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const headers = values[0].map((h, i) => hcNorm(h) || `COL_${i + 1}`);

  const headerMap = {
    data: hcFindHeader(headers, ["DATA", "Data"]),
    unidade: hcFindHeader(headers, ["UNIDADE", "Unidade", "WH", "Warehouse"]),
    colaborador: hcFindHeader(headers, ["COLABORADOR", "Colaborador", "NOME", "Nome"]),
    plantao: hcFindHeader(headers, ["PLANTÃO", "PLANTAO", "Plantão"]),
    turno: hcFindHeader(headers, ["TURNO", "Turno"]),
    ocorrencia: hcFindHeader(headers, ["OCORRÊNCIA", "OCORRENCIA", "Ocorrência"]),
    qtd: hcFindHeader(headers, ["QTD", "Quantidade"]),
    absenteismo: hcFindHeader(headers, ["ABSENTEÍSMO", "ABSENTEISMO", "%ABS", "ABS"]),
    observacoes: hcFindHeader(headers, ["OBSERVAÇÕES", "OBSERVACOES", "DESCONTO", "OBS"]),
  };

  const rows = values.slice(1).map((line) => {
    const raw = {};
    headers.forEach((header, i) => {
      raw[header] = line[i] ?? "";
    });

    return {
      DATA: headerMap.data ? raw[headerMap.data] || "" : "",
      UNIDADE: headerMap.unidade ? raw[headerMap.unidade] || "" : "",
      COLABORADOR: headerMap.colaborador ? raw[headerMap.colaborador] || "" : "",
      "PLANTÃO": headerMap.plantao ? raw[headerMap.plantao] || "" : "",
      TURNO: headerMap.turno ? raw[headerMap.turno] || "" : "",
      "OCORRÊNCIA": headerMap.ocorrencia ? raw[headerMap.ocorrencia] || "" : "",
      QTD: headerMap.qtd ? raw[headerMap.qtd] || "" : "1",
      "%ABS": headerMap.absenteismo ? raw[headerMap.absenteismo] || "" : "",
      "OBSERVAÇÕES": headerMap.observacoes ? raw[headerMap.observacoes] || "" : "",
      _raw: raw
    };
  }).filter((row) =>
    hcNorm(row.DATA) ||
    hcNorm(row.UNIDADE) ||
    hcNorm(row.COLABORADOR) ||
    hcNorm(row["PLANTÃO"]) ||
    hcNorm(row.TURNO) ||
    hcNorm(row["OCORRÊNCIA"])
  );

  console.log("HC AGENTES FBS headers detectados:", headerMap);
  console.log("HC AGENTES FBS linhas:", rows.length);

  return rows;
}

async function carregarHcAgentesFbsComCache() {
  const now = Date.now();

  if (hcAgentesCache && now - hcAgentesCacheTime < HC_AGENTES_CACHE_TTL) {
    return hcAgentesCache;
  }

  hcAgentesCache = await carregarHcAgentesFbsRaw();
  hcAgentesCacheTime = now;

  return hcAgentesCache;
}

app.get("/api/hc-agentes-fbs", requireAuth, async (req, res) => {
  try {
    const dados = await carregarHcAgentesFbsComCache();
    return res.json(dados);
  } catch (err) {
    console.log("Erro HC AGENTES FBS nova base:", err);
    return res.status(500).json({ error: "Erro ao carregar HC AGENTES FBS." });
  }
});

// ================== CHAMADA AGENTES FBS ==================

const CHAMADA_SPREADSHEET_ID = "1OLFfhaPKAL92co8vaHqpy_OiOU9oFEEqOrpgqo3nMJ0";
const CHAMADA_SHEET_NAME = "ABS AGENTES FBS";
const CHAMADA_RANGE = "'ABS AGENTES FBS'!A1:AZ";

app.get("/api/chamada/supervisores", requireAuth, async (req, res) => {
  try {
    const { agents } = await lerSheetChamada();

    const supervisores = [
      ...new Set(
        agents
          .filter((a) => !a.desligamento)
          .map((a) => a.lider)
          .filter(Boolean)
      ),
    ].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json(supervisores);
  } catch (error) {
    console.log("Erro /api/chamada/supervisores:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/chamada/agentes", requireAuth, async (req, res) => {
  try {
    const supervisor = String(req.query.supervisor || "").trim();

    if (!supervisor) {
      return res.status(400).json({ error: "Supervisor é obrigatório." });
    }

    const { agents } = await lerSheetChamada();

    const filtrados = agents
      .filter((a) => !a.desligamento)
      .filter((a) => normalizeText(a.lider) === normalizeText(supervisor))
      .sort((a, b) => a.colaborador.localeCompare(b.colaborador, "pt-BR"));

    return res.json(filtrados);
  } catch (error) {
    console.log("Erro /api/chamada/agentes:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.post("/api/chamada/salvar", requireAuth, async (req, res) => {
  try {
    const { data, supervisor, marcacoes } = req.body;

    if (!data || !supervisor || !Array.isArray(marcacoes)) {
      return res.status(400).json({
        error: "Campos obrigatórios: data, supervisor e marcacoes[].",
      });
    }

    const dataBR = /^\d{2}\/\d{2}\/\d{4}$/.test(String(data).trim())
      ? String(data).trim()
      : formatDateBR(data);

    if (!dataBR) {
      return res.status(400).json({ error: "Data inválida." });
    }

    const allowedStatus = ["P", "F", "DSR", "FE", "AT", "FC"];

    const { lateralDates, agents } = await lerSheetChamada();

    const targetDate = lateralDates.find(
      (d) => normalizeText(d.date) === normalizeText(dataBR)
    );

    if (!targetDate) {
      return res.status(404).json({
        error: `Data ${dataBR} não encontrada na planilha.`,
      });
    }

    const updates = [];

    for (const item of marcacoes) {
      const re = String(item.re || "").trim();
      const status = String(item.status || "").trim().toUpperCase();

      if (!re || !allowedStatus.includes(status)) continue;

      const agent = agents.find(
        (a) =>
          normalizeText(a.re) === normalizeText(re) &&
          normalizeText(a.lider) === normalizeText(supervisor)
      );

      if (!agent) continue;

      const a1Column = toA1Column(targetDate.colIndex);
      const a1Row = agent.rowIndex + 1;
      const range = `'${CHAMADA_SHEET_NAME}'!${a1Column}${a1Row}`;

      updates.push({
        range,
        values: [[status]],
      });
    }

    if (!updates.length) {
      return res.status(400).json({
        error: "Nenhuma marcação válida para salvar.",
      });
    }

    const sheets = await conectarSheetsEdicao();

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: CHAMADA_SPREADSHEET_ID,
      requestBody: {
        valueInputOption: "USER_ENTERED",
        data: updates,
      },
    });

    return res.json({
      success: true,
      message: `${updates.length} marcações salvas com sucesso.`,
      updates,
    });
  } catch (error) {
    console.log("Erro /api/chamada/salvar:", error.message);
    if (error.response?.data) {
      console.log("Detalhe Google:", error.response.data);
    }

    return res.status(500).json({
      error: error.message || "Erro ao salvar chamada.",
    });
  }
});

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
  const estado = accessNormalize(query.estado);
  const cidade = accessNormalize(query.cidade);
  const operacao = accessNormalize(query.operacao);
  const tipoLiberacao = accessNormalize(query.tipoLiberacao);
  const veiculo = accessNormalize(query.veiculo);
  const status = accessNormalize(query.status);
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

    const okEstado = !estado || accessNormalize(row[ACCESS_COLUMNS.estado]) === estado;
    const okCidade = !cidade || accessNormalize(row[ACCESS_COLUMNS.cidade]) === cidade;
    const okOperacao = !operacao || accessNormalize(row[ACCESS_COLUMNS.operacao]) === operacao;
    const okTipo =
      !tipoLiberacao ||
      accessNormalize(row[ACCESS_COLUMNS.tipoLiberacao]) === tipoLiberacao;
    const okVeiculo = !veiculo || accessNormalize(row[ACCESS_COLUMNS.veiculo]) === veiculo;
    const okStatus = !status || accessNormalize(row[ACCESS_COLUMNS.status]) === status;

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
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
      }),
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

    const statusMap = groupCountAccess(filtrados, ACCESS_COLUMNS.status);
    const estadoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.estado);
    const tipoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.tipoLiberacao);
    const operacaoMap = groupCountAccess(filtrados, ACCESS_COLUMNS.operacao);

    const dayMap = {};
    filtrados.forEach((row) => {
      if (!row._dataSolicitacao) return;
      const key = row._dataSolicitacao.toLocaleDateString("pt-BR");
      dayMap[key] = (dayMap[key] || 0) + 1;
    });

    const diaLabels = Object.keys(dayMap).sort((a, b) => {
      const da = parseDateBRAccess(a);
      const db = parseDateBRAccess(b);
      if (!da || !db) return 0;
      return da - db;
    });

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
      status: statusMap,
      estado: estadoMap,
      tipoLiberacao: tipoMap,
      operacao: operacaoMap,
      porDia: {
        labels: diaLabels,
        values: diaLabels.map((label) => dayMap[label]),
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

// ================== DESLIGADOS DASHBOARD ==================

app.get("/desligados", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "desligados.html"));
});

// COLOQUE AQUI O ID DA PLANILHA DE DESLIGADOS
const DESLIGADOS_SPREADSHEET_ID = "14U1f4ZdLKMWvARdkqmCxp4SOLuTdDjpA68UJsNwpXGY";
const DESLIGADOS_RANGE = "'Desligados'!A1:Z200000";

const DESLIGADOS_COLUMNS = {
  unidade: "UNIDADE",
  data: "DATA",
  nome: "Nome",
  cpf: "CPF",
  empresa: "EMPRESA",
  cargo: "CARGO",
  enviado: "Enviado Condominio para Bloqueio",
  bloqueio: "Bloqueio Efetivado",
  motivo: "Motivo de desligamento",
  controle: "Controle interno",
  detalhe: "Unnamed: 10",
};

let desligadosCache = null;
let desligadosCacheTime = 0;
const DESLIGADOS_CACHE_TTL = 5 * 60 * 1000;

function dNormalize(value) {
  return String(value || "").trim();
}

function dNormalizeLower(value) {
  return dNormalize(value).toLowerCase();
}

function parseDateBRDesligados(value) {
  const str = dNormalize(value);
  if (!str) return null;

  const match = str.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (!match) return null;

  const day = Number(match[1]);
  const month = Number(match[2]);
  let year = Number(match[3]);

  if (year < 1000) {
    year += 2000;
  }

  if (!day || !month || !year) return null;

  return new Date(year, month - 1, day);
}

function formatInputDate(date) {
  if (!date) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function carregarDesligadosRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: DESLIGADOS_SPREADSHEET_ID,
    range: DESLIGADOS_RANGE,
  });

  const values = response.data.values || [];

  if (!values.length || values.length < 2) {
    return [];
  }

  const headers = values[0].map((h) => dNormalize(h));
  const linhas = values.slice(1);

  const rows = linhas.map((linha) => {
    const obj = {};

    headers.forEach((header, index) => {
      obj[header] = linha[index] ?? "";
    });

    obj._data = parseDateBRDesligados(obj[DESLIGADOS_COLUMNS.data]);
    obj._mesRef = obj._data
      ? `${obj._data.getFullYear()}-${String(obj._data.getMonth() + 1).padStart(2, "0")}`
      : "";

    return obj;
  });

  return rows;
}

async function carregarDesligadosComCache() {
  const now = Date.now();

  if (desligadosCache && now - desligadosCacheTime < DESLIGADOS_CACHE_TTL) {
    return desligadosCache;
  }

  const data = await carregarDesligadosRaw();
  desligadosCache = data;
  desligadosCacheTime = now;

  console.log("DESLIGADOS cache atualizado:", data.length);

  return data;
}

function uniqueValues(data, column) {
  return [...new Set(data.map((row) => dNormalize(row[column])).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "pt-BR"));
}

function parseMultiValue(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => dNormalize(v))
    .filter(Boolean);
}

function matchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(dNormalize(fieldValue));
}

function filtrarDesligados(data, query) {
  const dataInicio = dNormalize(query.dataInicio);
  const dataFim = dNormalize(query.dataFim);

  const unidades = parseMultiValue(query.unidades);
  const empresas = parseMultiValue(query.empresas);
  const cargos = parseMultiValue(query.cargos);
  const enviados = parseMultiValue(query.enviados);
  const bloqueios = parseMultiValue(query.bloqueios);
  const motivos = parseMultiValue(query.motivos);
  const controles = parseMultiValue(query.controles);

  const busca = dNormalizeLower(query.busca);

  return data.filter((row) => {
    const dt = row._data;

    const okDataInicio = !dataInicio || (dt && formatInputDate(dt) >= dataInicio);
    const okDataFim = !dataFim || (dt && formatInputDate(dt) <= dataFim);

    const okUnidade = matchesMulti(row[DESLIGADOS_COLUMNS.unidade], unidades);
    const okEmpresa = matchesMulti(row[DESLIGADOS_COLUMNS.empresa], empresas);
    const okCargo = matchesMulti(row[DESLIGADOS_COLUMNS.cargo], cargos);
    const okEnviado = matchesMulti(row[DESLIGADOS_COLUMNS.enviado], enviados);
    const okBloqueio = matchesMulti(row[DESLIGADOS_COLUMNS.bloqueio], bloqueios);
    const okMotivo = matchesMulti(row[DESLIGADOS_COLUMNS.motivo], motivos);
    const okControle = matchesMulti(row[DESLIGADOS_COLUMNS.controle], controles);

    const searchable = [
      row[DESLIGADOS_COLUMNS.unidade],
      row[DESLIGADOS_COLUMNS.nome],
      row[DESLIGADOS_COLUMNS.cpf],
      row[DESLIGADOS_COLUMNS.empresa],
      row[DESLIGADOS_COLUMNS.cargo],
      row[DESLIGADOS_COLUMNS.motivo],
      row[DESLIGADOS_COLUMNS.controle],
      row[DESLIGADOS_COLUMNS.detalhe],
    ]
      .map(dNormalizeLower)
      .join(" ");

    const okBusca = !busca || searchable.includes(busca);

    return (
      okDataInicio &&
      okDataFim &&
      okUnidade &&
      okEmpresa &&
      okCargo &&
      okEnviado &&
      okBloqueio &&
      okMotivo &&
      okControle &&
      okBusca
    );
  });
}

function groupCount(data, column) {
  const map = {};
  data.forEach((row) => {
    const key = dNormalize(row[column]) || "Sem valor";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

app.get("/api/desligados-debug", requireAuth, async (req, res) => {
  try {
    const sheets = await conectarSheets();

    const meta = await sheets.spreadsheets.get({
      spreadsheetId: DESLIGADOS_SPREADSHEET_ID,
    });

    const abas = (meta.data.sheets || []).map(
      (s) => s.properties?.title || "SEM_NOME"
    );

    let values = [];
    let erroLeitura = null;

    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: DESLIGADOS_SPREADSHEET_ID,
        range: DESLIGADOS_RANGE,
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
      spreadsheetId: DESLIGADOS_SPREADSHEET_ID,
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

app.get("/api/desligados-filtros", requireAuth, async (req, res) => {
  try {
    const dados = await carregarDesligadosComCache();

    return res.json({
      unidades: uniqueValues(dados, DESLIGADOS_COLUMNS.unidade),
      empresas: uniqueValues(dados, DESLIGADOS_COLUMNS.empresa),
      cargos: uniqueValues(dados, DESLIGADOS_COLUMNS.cargo),
      enviados: uniqueValues(dados, DESLIGADOS_COLUMNS.enviado),
      bloqueios: uniqueValues(dados, DESLIGADOS_COLUMNS.bloqueio),
      motivos: uniqueValues(dados, DESLIGADOS_COLUMNS.motivo),
      controles: uniqueValues(dados, DESLIGADOS_COLUMNS.controle),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/desligados-resumo", requireAuth, async (req, res) => {
  try {
    const dados = await carregarDesligadosComCache();
    const filtrados = filtrarDesligados(dados, req.query);

    const total = filtrados.length;
    const enviadosSim = filtrados.filter(
      (r) => dNormalizeLower(r[DESLIGADOS_COLUMNS.enviado]) === "sim"
    ).length;
    const bloqueadosSim = filtrados.filter(
      (r) => dNormalizeLower(r[DESLIGADOS_COLUMNS.bloqueio]) === "sim"
    ).length;
    const pendentes = total - bloqueadosSim;
    const empresas = new Set(
      filtrados.map((r) => dNormalize(r[DESLIGADOS_COLUMNS.empresa])).filter(Boolean)
    ).size;
    const unidades = new Set(
      filtrados.map((r) => dNormalize(r[DESLIGADOS_COLUMNS.unidade])).filter(Boolean)
    ).size;
    const taxaBloqueio = total ? (bloqueadosSim / total) * 100 : 0;

    return res.json({
      total,
      enviadosSim,
      bloqueadosSim,
      pendentes,
      empresas,
      unidades,
      taxaBloqueio,
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
      }),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/desligados-graficos", requireAuth, async (req, res) => {
  try {
    const dados = await carregarDesligadosComCache();
    const filtrados = filtrarDesligados(dados, req.query);

    const enviadosMap = groupCount(filtrados, DESLIGADOS_COLUMNS.enviado);
    const bloqueioMap = groupCount(filtrados, DESLIGADOS_COLUMNS.bloqueio);
    const unidadeMap = groupCount(filtrados, DESLIGADOS_COLUMNS.unidade);
    const empresaMap = groupCount(filtrados, DESLIGADOS_COLUMNS.empresa);
    const motivoMap = groupCount(filtrados, DESLIGADOS_COLUMNS.motivo);

    const monthMap = {};
    filtrados.forEach((row) => {
      if (!row._mesRef) return;
      monthMap[row._mesRef] = (monthMap[row._mesRef] || 0) + 1;
    });

    const monthLabels = Object.keys(monthMap).sort();

    const tabelaUnidade = {};
    const tabelaEmpresa = {};

    filtrados.forEach((row) => {
      const unidade = dNormalize(row[DESLIGADOS_COLUMNS.unidade]) || "Sem unidade";
      const empresa = dNormalize(row[DESLIGADOS_COLUMNS.empresa]) || "Sem empresa";
      const enviado = dNormalizeLower(row[DESLIGADOS_COLUMNS.enviado]) === "sim";
      const bloqueado = dNormalizeLower(row[DESLIGADOS_COLUMNS.bloqueio]) === "sim";

      if (!tabelaUnidade[unidade]) {
        tabelaUnidade[unidade] = { total: 0, enviados: 0, bloqueados: 0 };
      }

      if (!tabelaEmpresa[empresa]) {
        tabelaEmpresa[empresa] = { total: 0, enviados: 0, bloqueados: 0 };
      }

      tabelaUnidade[unidade].total += 1;
      tabelaEmpresa[empresa].total += 1;

      if (enviado) {
        tabelaUnidade[unidade].enviados += 1;
        tabelaEmpresa[empresa].enviados += 1;
      }

      if (bloqueado) {
        tabelaUnidade[unidade].bloqueados += 1;
        tabelaEmpresa[empresa].bloqueados += 1;
      }
    });

    return res.json({
      enviados: enviadosMap,
      bloqueio: bloqueioMap,
      unidade: unidadeMap,
      empresa: empresaMap,
      motivo: motivoMap,
      porMes: {
        labels: monthLabels,
        values: monthLabels.map((label) => monthMap[label]),
      },
      tabelaUnidade: Object.entries(tabelaUnidade)
        .sort((a, b) => b[1].total - a[1].total)
        .slice(0, 50)
        .map(([unidade, info]) => ({
          unidade,
          total: info.total,
          enviados: info.enviados,
          bloqueados: info.bloqueados,
        })),
      tabelaEmpresa: Object.entries(tabelaEmpresa)
        .sort((a, b) => b[1].total - a[1].total)
        .slice(0, 50)
        .map(([empresa, info]) => ({
          empresa,
          total: info.total,
          enviados: info.enviados,
          bloqueados: info.bloqueados,
        })),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/desligados-detalhes", requireAuth, async (req, res) => {
  try {
    const dados = await carregarDesligadosComCache();
    const filtrados = filtrarDesligados(dados, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const start = (page - 1) * limit;
    const end = start + limit;

    const rows = filtrados.slice(start, end).map((row) => ({
      [DESLIGADOS_COLUMNS.unidade]: row[DESLIGADOS_COLUMNS.unidade] || "",
      [DESLIGADOS_COLUMNS.data]: row[DESLIGADOS_COLUMNS.data] || "",
      [DESLIGADOS_COLUMNS.nome]: row[DESLIGADOS_COLUMNS.nome] || "",
      [DESLIGADOS_COLUMNS.cpf]: row[DESLIGADOS_COLUMNS.cpf] || "",
      [DESLIGADOS_COLUMNS.empresa]: row[DESLIGADOS_COLUMNS.empresa] || "",
      [DESLIGADOS_COLUMNS.cargo]: row[DESLIGADOS_COLUMNS.cargo] || "",
      [DESLIGADOS_COLUMNS.enviado]: row[DESLIGADOS_COLUMNS.enviado] || "",
      [DESLIGADOS_COLUMNS.bloqueio]: row[DESLIGADOS_COLUMNS.bloqueio] || "",
      [DESLIGADOS_COLUMNS.motivo]: row[DESLIGADOS_COLUMNS.motivo] || "",
      [DESLIGADOS_COLUMNS.controle]: row[DESLIGADOS_COLUMNS.controle] || "",
      detalhe: row[DESLIGADOS_COLUMNS.detalhe] || "",
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
    });
  }
});

// ================== CCO FBS DASHBOARD ==================

// ================== CCO FBS DASHBOARD ==================

// ================== CCO FBS DASHBOARD ==================

app.get("/cco-fbs", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "cco-fbs.html"));
});

const CCO_FBS_SPREADSHEET_ID = "122nrPqL6ajMDIMzLZiNwacpCVk9BjpFMM30a0a-t8ws";

let ccoCache = null;
let ccoCacheTime = 0;
let ccoSheetTitle = null;
const CCO_CACHE_TTL = 5 * 60 * 1000;

function ccoNormalize(value) {
  return String(value || "").trim();
}

function ccoNormalizeLower(value) {
  return ccoNormalize(value).toLowerCase();
}

function ccoParseDate(value) {
  const str = ccoNormalize(value);
  if (!str) return null;

  if (/^\d{2}\/\d{2}\/\d{4}/.test(str)) {
    const [datePart] = str.split(" ");
    const [day, month, year] = datePart.split("/").map(Number);
    return new Date(year, month - 1, day);
  }

  if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
    const [datePart] = str.split(" ");
    const [year, month, day] = datePart.split("-").map(Number);
    return new Date(year, month - 1, day);
  }

  return null;
}

function ccoFormatInputDate(date) {
  if (!date) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function ccoIsUsefulHeader(header) {
  const h = ccoNormalize(header);
  return h && !/^unnamed/i.test(h);
}

function ccoIsSecurityHeader(header) {
  const h = ccoNormalizeLower(header);
  return /^security\s*\d+$/i.test(h) || /analista security/i.test(h);
}

function ccoLooksLikeDateHeader(header) {
  const h = ccoNormalizeLower(header);
  return /data|date|dia/.test(h);
}

function ccoFindHeader(headers, possibilities) {
  for (const possibility of possibilities) {
    const found = headers.find((h) => ccoNormalizeLower(h) === ccoNormalizeLower(possibility));
    if (found) return found;
  }
  return null;
}

async function ccoGetBestSheetTitle() {
  if (ccoSheetTitle) return ccoSheetTitle;

  const sheets = await conectarSheets();
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: CCO_FBS_SPREADSHEET_ID,
  });

  const abas = (meta.data.sheets || [])
    .map((s) => s.properties?.title)
    .filter(Boolean);

  let bestTitle = abas[0] || "Página1";
  let bestScore = -1;

  for (const title of abas) {
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: CCO_FBS_SPREADSHEET_ID,
        range: `'${1Solicitações de imagens}'!A1:AZ2000`,
      });

      const values = response.data.values || [];
      if (!values.length || values.length < 2) continue;

      const headers = (values[0] || []).map((h) => ccoNormalize(h));
      const usefulHeaders = headers.filter(
        (header) => ccoIsUsefulHeader(header) && !ccoIsSecurityHeader(header)
      );

      const rows = values.slice(1).filter((r) => r.some((cell) => ccoNormalize(cell)));
      const score = rows.length * 100 + usefulHeaders.length;

      if (score > bestScore) {
        bestScore = score;
        bestTitle = title;
      }
    } catch (error) {
      console.log("Erro avaliando aba CCO:", title, error.message);
    }
  }

  ccoSheetTitle = bestTitle;
  console.log("CCO FBS aba escolhida:", ccoSheetTitle);
  return ccoSheetTitle;
}

async function ccoLoadRaw() {
  const sheets = await conectarSheets();
  const sheetTitle = await ccoGetBestSheetTitle();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CCO_FBS_SPREADSHEET_ID,
    range: `'${sheetTitle}'!A1:AZ200000`,
  });

  const values = response.data.values || [];

  if (!values.length || values.length < 2) {
    return { headers: [], rows: [], sheetTitle };
  }

  const headers = (values[0] || []).map((h, idx) => {
    const name = ccoNormalize(h);
    return name || `COL_${idx + 1}`;
  });

  const rows = values.slice(1).map((line) => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = line[i] ?? "";
    });
    return obj;
  });

  return { headers, rows, sheetTitle };
}

async function ccoLoadWithCache() {
  const now = Date.now();

  if (ccoCache && now - ccoCacheTime < CCO_CACHE_TTL) {
    return ccoCache;
  }

  const { headers, rows, sheetTitle } = await ccoLoadRaw();

  const cleanHeaders = headers.filter(
    (header) => ccoIsUsefulHeader(header) && !ccoIsSecurityHeader(header)
  );

  const cleanRows = rows
    .map((row) => {
      const obj = {};
      cleanHeaders.forEach((header) => {
        obj[header] = row[header] ?? "";
      });
      return obj;
    })
    .filter((row) => cleanHeaders.some((header) => ccoNormalize(row[header])));

  const dateColumns = cleanHeaders.filter((header) => {
    if (!ccoLooksLikeDateHeader(header)) return false;
    const sample = cleanRows.find((r) => ccoNormalize(r[header]));
    return sample ? !!ccoParseDate(sample[header]) : false;
  });

  const primaryDateColumn = dateColumns[0] || null;

  const enrichedRows = cleanRows.map((row) => ({
    ...row,
    _primaryDate: primaryDateColumn ? ccoParseDate(row[primaryDateColumn]) : null,
  }));

  const categoricalHeaders = cleanHeaders.filter((header) => {
    const distinct = new Set(
      enrichedRows
        .map((r) => ccoNormalize(r[header]))
        .filter(Boolean)
        .slice(0, 5000)
    );
    return distinct.size > 1 && distinct.size <= 300;
  });

  const unidadeHeader = ccoFindHeader(cleanHeaders, [
    "ID Unidade",
    "Unidade",
    "Base",
    "Site",
    "Operação",
  ]);

  const statusHeader = ccoFindHeader(cleanHeaders, [
    "Status",
    "Situação",
    "Situacao",
    "Status da solicitação",
    "Status das Solicitações",
  ]);

  const solicitanteHeader = ccoFindHeader(cleanHeaders, [
    "E-mail do solicitante",
    "Solicitante",
    "Nome do solicitante",
    "Requisitante",
  ]);

  ccoCache = {
    sheetTitle,
    headers: cleanHeaders,
    rows: enrichedRows,
    primaryDateColumn,
    dateColumns,
    categoricalHeaders,
    unidadeHeader,
    statusHeader,
    solicitanteHeader,
  };

  ccoCacheTime = now;

  console.log("CCO cache atualizado:", {
    aba: sheetTitle,
    totalLinhas: enrichedRows.length,
    totalColunas: cleanHeaders.length,
    dataPrincipal: primaryDateColumn,
    unidadeHeader,
    statusHeader,
    solicitanteHeader,
  });

  return ccoCache;
}

function ccoParseMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => ccoNormalize(v))
    .filter(Boolean);
}

function ccoApplyFilters(rows, query, filterHeaders) {
  const dataInicio = ccoNormalize(query.dataInicio);
  const dataFim = ccoNormalize(query.dataFim);
  const busca = ccoNormalizeLower(query.busca);

  return rows.filter((row) => {
    const dt = row._primaryDate;

    const okDataInicio = !dataInicio || (dt && ccoFormatInputDate(dt) >= dataInicio);
    const okDataFim = !dataFim || (dt && ccoFormatInputDate(dt) <= dataFim);

    if (!okDataInicio || !okDataFim) return false;

    for (const header of filterHeaders) {
      const selected = ccoParseMulti(query[`f_${header}`]);
      if (selected.length && !selected.includes(ccoNormalize(row[header]))) {
        return false;
      }
    }

    if (busca) {
      const searchable = Object.entries(row)
        .filter(([key]) => !key.startsWith("_"))
        .map(([, value]) => ccoNormalizeLower(value))
        .join(" ");

      if (!searchable.includes(busca)) return false;
    }

    return true;
  });
}

function ccoGroupCount(data, header) {
  const map = {};
  data.forEach((row) => {
    const key = ccoNormalize(row[header]) || "Sem valor";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

app.get("/api/cco-fbs-debug", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();
    return res.json({
      ok: true,
      abaUsada: meta.sheetTitle,
      totalLinhas: meta.rows.length,
      totalColunas: meta.headers.length,
      primaryDateColumn: meta.primaryDateColumn,
      unidadeHeader: meta.unidadeHeader,
      statusHeader: meta.statusHeader,
      solicitanteHeader: meta.solicitanteHeader,
      filtrosDetectados: meta.categoricalHeaders.slice(0, 20),
      primeiraLinha: meta.rows[0] || null,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

app.get("/api/cco-fbs-filtros", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();

    const filtros = meta.categoricalHeaders.slice(0, 8).map((header) => ({
      key: header,
      label: header,
      values: [...new Set(
        meta.rows.map((r) => ccoNormalize(r[header])).filter(Boolean)
      )].sort((a, b) => a.localeCompare(b, "pt-BR")),
    }));

    return res.json({
      sheetTitle: meta.sheetTitle,
      dateColumn: meta.primaryDateColumn,
      filtros,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/cco-fbs-resumo", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();
    const filterHeaders = meta.categoricalHeaders.slice(0, 8);
    const filtrados = ccoApplyFilters(meta.rows, req.query, filterHeaders);

    const total = filtrados.length;
    const totalUnidades = meta.unidadeHeader
      ? new Set(
          filtrados.map((r) => ccoNormalize(r[meta.unidadeHeader])).filter(Boolean)
        ).size
      : 0;

    const ocorrencias = meta.statusHeader
      ? Object.entries(ccoGroupCount(filtrados, meta.statusHeader))
          .filter(([k]) => /recus|negad|reprov|bloque|pendente|anal/i.test(k))
          .reduce((acc, [, v]) => acc + v, 0)
      : 0;

    let solicitanteLider = "-";
    if (meta.solicitanteHeader) {
      const top = Object.entries(ccoGroupCount(filtrados, meta.solicitanteHeader))
        .sort((a, b) => b[1] - a[1])[0];
      solicitanteLider = top ? `${top[0]} (${top[1]})` : "-";
    }

    return res.json({
      total,
      totalUnidades,
      ocorrencias,
      solicitanteLider,
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
      }),
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/cco-fbs-graficos", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();
    const filterHeaders = meta.categoricalHeaders.slice(0, 8);
    const filtrados = ccoApplyFilters(meta.rows, req.query, filterHeaders);

    const preferredHeaders = [
      ccoFindHeader(meta.headers, ["Tipo de solicitação", "Tipo de Solicitação"]),
      ccoFindHeader(meta.headers, ["Ocorrência", "Ocorrencia", "Motivo", "Motivo de recusa"]),
      ccoFindHeader(meta.headers, ["E-mail do solicitante", "Requisitante", "Solicitante"]),
      ccoFindHeader(meta.headers, ["Unidade", "ID Unidade"]),
      ccoFindHeader(meta.headers, ["Status", "Status da solicitação", "Status das Solicitações"]),
      ccoFindHeader(meta.headers, ["Setor"]),
    ].filter(Boolean);

    const uniquePreferred = [...new Set(preferredHeaders)];
    const fallbackHeaders = meta.categoricalHeaders.filter((h) => !uniquePreferred.includes(h));
    const finalHeaders = [...uniquePreferred, ...fallbackHeaders].slice(0, 6);

    const makeTop = (header, limit = 10) => {
      if (!header) return { header: "Sem dados", labels: [], values: [] };
      const grouped = ccoGroupCount(filtrados, header);
      const ordered = Object.entries(grouped)
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit);

      return {
        header,
        labels: ordered.map((x) => x[0]),
        values: ordered.map((x) => x[1]),
      };
    };

    const charts = finalHeaders.map((header, idx) => makeTop(header, idx < 3 ? 12 : 10));

    const byDay = {};
    filtrados.forEach((row) => {
      if (!row._primaryDate) return;
      const key = row._primaryDate.toLocaleDateString("pt-BR");
      byDay[key] = (byDay[key] || 0) + 1;
    });

    const dayLabels = Object.keys(byDay).sort((a, b) => {
      const da = ccoParseDate(a);
      const db = ccoParseDate(b);
      if (!da || !db) return 0;
      return da - db;
    });

    return res.json({
      charts,
      porDia: {
        labels: dayLabels,
        values: dayLabels.map((k) => byDay[k]),
      },
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/cco-fbs-detalhes", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();
    const filterHeaders = meta.categoricalHeaders.slice(0, 8);
    const filtrados = ccoApplyFilters(meta.rows, req.query, filterHeaders);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const start = (page - 1) * limit;
    const end = start + limit;

    const rows = filtrados.slice(start, end).map((row) => {
      const obj = {};
      meta.headers.forEach((header) => {
        obj[header] = row[header] || "";
      });
      return obj;
    });

    return res.json({
      headers: meta.headers,
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
    });
  }
});

// ================== FIRST MILE ACCESS DASHBOARD ==================

app.get("/first-mile-access", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "first-mile-access.html"));
});

const FM_ACCESS_SPREADSHEET_ID = "1kxePCOZeaVh48C7tP7Ua_iUpYg7EJttRvC7VAkEfQBo";

let fmAccessCache = null;
let fmAccessCacheTime = 0;
const FM_ACCESS_CACHE_TTL = 5 * 60 * 1000;

function fmNorm(v) {
  return String(v || "").trim();
}

function fmNormLower(v) {
  return fmNorm(v).toLowerCase();
}

function fmParseDate(value) {
  const str = fmNorm(value);
  if (!str) return null;

  if (/^\d{2}\/\d{2}\/\d{4}/.test(str)) {
    const [datePart] = str.split(" ");
    const [day, month, year] = datePart.split("/").map(Number);
    return new Date(year, month - 1, day);
  }

  if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
    const [datePart] = str.split(" ");
    const [year, month, day] = datePart.split("-").map(Number);
    return new Date(year, month - 1, day);
  }

  return null;
}

function fmFormatISO(date) {
  if (!date) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function fmMonthLabel(date) {
  const meses = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
  ];
  return meses[date.getMonth()];
}

function fmFindHeader(headers, possibilities) {
  for (const possibility of possibilities) {
    const found = headers.find((h) => fmNormLower(h) === fmNormLower(possibility));
    if (found) return found;
  }

  for (const possibility of possibilities) {
    const found = headers.find((h) => fmNormLower(h).includes(fmNormLower(possibility)));
    if (found) return found;
  }

  return null;
}

function fmGroupCount(rows, field, uniqueBy = null) {
  const map = {};

  rows.forEach((row) => {
    const key = fmNorm(row[field]) || "Sem valor";

    if (!uniqueBy) {
      map[key] = (map[key] || 0) + 1;
      return;
    }

    if (!map[key]) map[key] = new Set();
    const uniq = fmNorm(row[uniqueBy]);
    if (uniq) map[key].add(uniq);
  });

  if (!uniqueBy) return map;

  const normalized = {};
  Object.keys(map).forEach((k) => {
    normalized[k] = map[k].size;
  });
  return normalized;
}

function fmToTopList(mapObj, limit = 10) {
  return Object.entries(mapObj)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([nome, total]) => ({ nome, total }));
}

function fmApplyFilters(rows, query) {
  const dataInicio = fmNorm(query.dataInicio);
  const dataFim = fmNorm(query.dataFim);
  const busca = fmNormLower(query.busca);

  const agency = fmNorm(query.agency);
  const regional = fmNorm(query.regional);
  const city = fmNorm(query.city);
  const vehicle = fmNorm(query.vehicle);
  const monitor = fmNorm(query.monitor);

  return rows.filter((row) => {
    if (!row._date) return false;

    const iso = fmFormatISO(row._date);
    if (dataInicio && iso < dataInicio) return false;
    if (dataFim && iso > dataFim) return false;

    if (agency && fmNorm(row["Agency"]) !== agency) return false;
    if (regional && fmNorm(row["Regional"]) !== regional) return false;
    if (city && fmNorm(row["City"]) !== city) return false;
    if (vehicle && fmNorm(row["Vehicle"]) !== vehicle) return false;
    if (monitor && fmNorm(row["NOME DO MONITOR"]) !== monitor) return false;

    if (busca) {
      const text = [
        row["Agency"],
        row["Regional"],
        row["Route Name"],
        row["Shop Name"],
        row["City"],
        row["Driver Name"],
        row["NOME DO MONITOR"],
        row["Status Final"],
        row["Status Validação"],
        row["Ocorrência Jira"]
      ].map(fmNormLower).join(" ");

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

async function fmAccessLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: FM_ACCESS_SPREADSHEET_ID,
    range: "'Painel Operacional'!A1:AZ200000",
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) {
    return { headers: [], rows: [] };
  }

  const headers = values[0].map((h, idx) => fmNorm(h) || `COL_${idx + 1}`);

  const rows = values.slice(1).map((line) => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = line[i] ?? "";
    });
    obj._date = fmParseDate(obj["Data Coleta"]);
    return obj;
  }).filter((row) => row._date && fmNorm(row["Route Name"]));

  return { headers, rows };
}

async function fmAccessLoadWithCache() {
  const now = Date.now();

  if (fmAccessCache && now - fmAccessCacheTime < FM_ACCESS_CACHE_TTL) {
    return fmAccessCache;
  }

  const data = await fmAccessLoadRaw();
  fmAccessCache = data;
  fmAccessCacheTime = now;

  return data;
}

app.get("/api/fm-access-filtros", requireAuth, async (req, res) => {
  try {
    const { rows } = await fmAccessLoadWithCache();

    const agencies = [...new Set(rows.map((r) => fmNorm(r["Agency"])).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const regionals = [...new Set(rows.map((r) => fmNorm(r["Regional"])).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const cities = [...new Set(rows.map((r) => fmNorm(r["City"])).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const vehicles = [...new Set(rows.map((r) => fmNorm(r["Vehicle"])).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const monitors = [...new Set(rows.map((r) => fmNorm(r["NOME DO MONITOR"])).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json({
      agencies,
      regionals,
      cities,
      vehicles,
      monitors,
    });
  } catch (error) {
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/fm-access-graficos", requireAuth, async (req, res) => {
  try {
    const { rows } = await fmAccessLoadWithCache();
    const filtrados = fmApplyFilters(rows, req.query);

    const totalRotas = new Set(filtrados.map((r) => fmNorm(r["Route Name"])).filter(Boolean)).size;
    const totalSellers = new Set(filtrados.map((r) => fmNorm(r["Shop ID"])).filter(Boolean)).size;
    const totalCities = new Set(filtrados.map((r) => fmNorm(r["City"])).filter(Boolean)).size;
    const totalDrivers = new Set(filtrados.map((r) => fmNorm(r["Driver Name"])).filter(Boolean)).size;
    const totalOcorrencias = filtrados.filter((r) => {
      const v = fmNormLower(r["Ocorrência Jira"]);
      return v && v !== "ok" && v !== "sem ocorrência";
    }).length;

    const porAgency = fmToTopList(fmGroupCount(filtrados, "Agency", "Route Name"), 10);
    const porRegional = fmToTopList(fmGroupCount(filtrados, "Regional", "Route Name"), 10);
    const porStatusFinal = fmToTopList(fmGroupCount(filtrados, "Status Final", "Route Name"), 10);
    const porVehicle = fmToTopList(fmGroupCount(filtrados, "Vehicle", "Route Name"), 10);
    const porMonitor = fmToTopList(fmGroupCount(filtrados, "NOME DO MONITOR", "Route Name"), 10);
    const porCity = fmToTopList(fmGroupCount(filtrados, "City", "Route Name"), 12);

    const distribuicao = [
      {
        nome: "Validado",
        total: filtrados.filter((r) => fmNormLower(r["Validação Ocorrência"]) === "validado").length
      },
      {
        nome: "Divergente",
        total: filtrados.filter((r) => fmNormLower(r["Validação Ocorrência"]) === "divergente").length
      }
    ];

    const byDay = {};
    filtrados.forEach((row) => {
      const day = String(row._date.getDate()).padStart(2, "0");
      byDay[day] = (byDay[day] || 0) + 1;
    });
    const dayLabels = Object.keys(byDay).sort((a, b) => Number(a) - Number(b));

    const byMonth = {};
    filtrados.forEach((row) => {
      const monthIdx = row._date.getMonth();
      byMonth[monthIdx] = (byMonth[monthIdx] || 0) + 1;
    });
    const monthIndexes = Object.keys(byMonth).map(Number).sort((a, b) => a - b);

    return res.json({
      resumo: {
        totalRotas,
        totalSellers,
        totalCities,
        totalDrivers,
        totalOcorrencias
      },
      porAgency,
      porRegional,
      porStatusFinal,
      porVehicle,
      porMonitor,
      porCity,
      distribuicao,
      porDia: {
        labels: dayLabels,
        values: dayLabels.map((d) => byDay[d]),
      },
      porMes: {
        labels: monthIndexes.map((m) => fmMonthLabel(new Date(2025, m, 1))),
        values: monthIndexes.map((m) => byMonth[m]),
      }
    });
  } catch (error) {
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/fm-access-detalhes", requireAuth, async (req, res) => {
  try {
    const { rows } = await fmAccessLoadWithCache();
    const filtrados = fmApplyFilters(rows, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const start = (page - 1) * limit;
    const end = start + limit;

    const headers = [
      "Data Coleta",
      "Agency",
      "Regional",
      "Route Name",
      "City",
      "Vehicle",
      "Driver Name",
      "NOME DO MONITOR",
      "Status Validação",
      "Ocorrência Jira",
      "Status Final"
    ];

    const pageRows = filtrados.slice(start, end).map((row) => {
      const obj = {};
      headers.forEach((h) => {
        obj[h] = row[h] || "";
      });
      return obj;
    });

    return res.json({
      headers,
      rows: pageRows,
      total: filtrados.length,
      page,
      limit,
      totalPages: Math.ceil(filtrados.length / limit),
    });
  } catch (error) {
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== SERVIDOR ==================

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando:");
  console.log(`http://localhost:${PORT}`);
});
