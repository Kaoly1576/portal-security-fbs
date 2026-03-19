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
    secret: process.env.SESSION_SECRET || "segredo_super_secreto",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
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
  status TEXT DEFAULT 'pendente'
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

passport.use(
  new GoogleStrategy(
    {
      clientID: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      callbackURL:
        process.env.GOOGLE_CALLBACK_URL ||
        "http://localhost:3000/auth/google/callback",
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

// ================== HELPERS ==================

function requireAuth(req, res, next) {
  if (!req.session.userId) return res.redirect("/login");
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

app.get(
  "/auth/google",
  passport.authenticate("google", {
    scope: ["openid", "profile", "email"],
    session: false,
  })
);

app.get(
  "/auth/google/callback",
  passport.authenticate("google", {
    failureRedirect: "/login",
    session: false,
  }),
  (req, res) => {
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
  }
);

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

    const senhaValida = await bcrypt.compare(senha, user.senha);
    if (!senhaValida) return res.send("Senha incorreta.");

    req.session.userId = user.id;
    req.session.nome = user.nome;
    req.session.email = user.email;
    req.session.perfil = user.perfil;
    req.session.foto = "";

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

app.get("/api/hc-agentes-fbs", requireAuth, async (req, res) => {
  try {
    const sheets = await conectarSheets();

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: "1OLFfhaPKAL92co8vaHqpy_OiOU9oFEEqOrpgqo3nMJ0",
      range: "'ABS AGENTES FBS'!A1:AZ",
    });

    const rows = response.data.values || [];

    if (!rows.length) {
      return res.json([]);
    }

    const isDate = (value) => /^\d{2}\/\d{2}\/\d{4}$/.test(String(value || "").trim());

    const DATE_ROW_INDEX = rows.findIndex((row) => {
      const totalDatas = row.filter((cell) => isDate(cell)).length;
      return totalDatas >= 5;
    });

    if (DATE_ROW_INDEX === -1) {
      console.log("Não encontrei a linha de datas na planilha.");
      return res.json([]);
    }

    const dateRow = rows[DATE_ROW_INDEX] || [];

    const FIXED_COLS = dateRow.findIndex((cell) => isDate(cell));

    if (FIXED_COLS === -1) {
      console.log("Não encontrei a primeira coluna de datas.");
      return res.json([]);
    }

    const DATA_START_ROW = DATE_ROW_INDEX + 3;

    const lateralDates = dateRow
      .slice(FIXED_COLS)
      .map((d) => String(d || "").trim());

    const lista = rows
      .slice(DATA_START_ROW)
      .filter((r) => (r[0] || r[1] || r[6]))
      .map((r) => {
        const dias = lateralDates.map((data, idx) => ({
          data,
          valor: String(r[FIXED_COLS + idx] || "").trim().toUpperCase(),
        }));

        return {
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
          dias,
        };
      });

    return res.json(lista);
  } catch (err) {
    console.log("Erro HC AGENTES FBS:", err);
    return res.json([]);
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

function ccoLooksLikeDateHeader(header) {
  const h = ccoNormalizeLower(header);
  return /data|date|dia/.test(h);
}

function ccoIsSecurityAnalystHeader(header) {
  const h = ccoNormalizeLower(header);
  return /^security\s*\d+$/i.test(h) || /analista security/i.test(h);
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

  const abas = (meta.data.sheets || []).map((s) => s.properties?.title).filter(Boolean);

  let bestTitle = abas[0] || "Página1";
  let bestScore = -1;

  for (const title of abas) {
    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: CCO_FBS_SPREADSHEET_ID,
        range: '"Solicitações de imagens"!A1:AZ2000',
      });

      const values = response.data.values || [];
      if (!values.length) continue;

      const headers = values[0].map((h) => ccoNormalize(h));
      const usefulHeaders = headers.filter(
        (header) => ccoIsUsefulHeader(header) && !ccoIsSecurityAnalystHeader(header)
      );

      const rowCount = Math.max(values.length - 1, 0);
      const score = rowCount * 100 + usefulHeaders.length;

      if (score > bestScore) {
        bestScore = score;
        bestTitle = title;
      }
    } catch (e) {
      console.log("Erro ao avaliar aba CCO:", title, e.message);
    }
  }

  ccoSheetTitle = bestTitle;
  console.log("CCO aba escolhida:", ccoSheetTitle);
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

  const rawHeaders = values[0].map((h, idx) => {
    const header = ccoNormalize(h);
    return header || `COL_${idx + 1}`;
  });

  const rows = values.slice(1).map((line) => {
    const obj = {};
    rawHeaders.forEach((header, i) => {
      obj[header] = line[i] ?? "";
    });
    return obj;
  });

  return { headers: rawHeaders, rows, sheetTitle };
}

async function ccoLoadWithCache() {
  const now = Date.now();

  if (ccoCache && now - ccoCacheTime < CCO_CACHE_TTL) {
    return ccoCache;
  }

  const { headers, rows, sheetTitle } = await ccoLoadRaw();

  const usefulHeaders = headers.filter(
    (header) => ccoIsUsefulHeader(header) && !ccoIsSecurityAnalystHeader(header)
  );

  const detectedDateColumns = usefulHeaders.filter((header) => {
    if (!ccoLooksLikeDateHeader(header)) return false;
    const sample = rows.find((r) => ccoNormalize(r[header]));
    return sample ? !!ccoParseDate(sample[header]) : false;
  });

  const primaryDateColumn = detectedDateColumns[0] || null;

  const enrichedRows = rows
    .map((row) => {
      const cleanRow = {};
      usefulHeaders.forEach((header) => {
        cleanRow[header] = row[header] ?? "";
      });

      cleanRow._primaryDate = primaryDateColumn ? ccoParseDate(cleanRow[primaryDateColumn]) : null;
      return cleanRow;
    })
    .filter((row) => {
      const filled = usefulHeaders.filter((header) => ccoNormalize(row[header])).length;
      return filled > 0;
    });

  const categoricalHeaders = usefulHeaders.filter((header) => {
    const distinct = new Set(
      enrichedRows.map((r) => ccoNormalize(r[header])).filter(Boolean).slice(0, 5000)
    );
    return distinct.size > 1 && distinct.size <= 300;
  });

  const unidadeHeader = ccoFindHeader(usefulHeaders, [
    "ID Unidade",
    "Unidade",
    "Site",
    "Base",
    "Operação",
  ]);

  const statusHeader = ccoFindHeader(usefulHeaders, [
    "Status",
    "Situação",
    "Situacao",
  ]);

  ccoCache = {
    sheetTitle,
    headers: usefulHeaders,
    rows: enrichedRows,
    primaryDateColumn,
    dateColumns: detectedDateColumns,
    categoricalHeaders,
    unidadeHeader,
    statusHeader,
  };

  ccoCacheTime = now;

  console.log("CCO cache atualizado:", {
    sheetTitle,
    totalRows: enrichedRows.length,
    totalHeaders: usefulHeaders.length,
    primaryDateColumn,
    unidadeHeader,
    statusHeader,
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

function ccoApplyFilters(data, query, allowedHeaders) {
  const dataInicio = ccoNormalize(query.dataInicio);
  const dataFim = ccoNormalize(query.dataFim);
  const busca = ccoNormalizeLower(query.busca);

  return data.filter((row) => {
    const dt = row._primaryDate;

    const okDataInicio = !dataInicio || (dt && ccoFormatInputDate(dt) >= dataInicio);
    const okDataFim = !dataFim || (dt && ccoFormatInputDate(dt) <= dataFim);

    if (!okDataInicio || !okDataFim) return false;

    for (const header of allowedHeaders) {
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
      filtrosDetectados: meta.categoricalHeaders.slice(0, 20),
      primeiraLinha: meta.rows[0] || null,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});

app.get("/api/cco-fbs-filtros", requireAuth, async (req, res) => {
  try {
    const meta = await ccoLoadWithCache();

    const filtros = meta.categoricalHeaders.slice(0, 8).map((header) => ({
      key: header,
      label: header,
      values: [...new Set(meta.rows.map((r) => ccoNormalize(r[header])).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR")),
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
      ? new Set(filtrados.map((r) => ccoNormalize(r[meta.unidadeHeader])).filter(Boolean)).size
      : 0;

    const statusMap = meta.statusHeader ? ccoGroupCount(filtrados, meta.statusHeader) : {};

    const ativos = Object.entries(statusMap)
      .filter(([key]) => /ativo/i.test(key) && !/inativo/i.test(key))
      .reduce((acc, [, val]) => acc + val, 0);

    const pendentes = Object.entries(statusMap)
      .filter(([key]) => /pendente/i.test(key))
      .reduce((acc, [, val]) => acc + val, 0);

    const inativos = Object.entries(statusMap)
      .filter(([key]) => /inativo|desligado|bloqueado/i.test(key))
      .reduce((acc, [, val]) => acc + val, 0);

    const topStatus = Object.entries(statusMap).sort((a, b) => b[1] - a[1])[0] || null;

    return res.json({
      total,
      totalUnidades,
      ativos,
      pendentes,
      inativos,
      statusDominante: topStatus ? `${topStatus[0]} (${topStatus[1]})` : "-",
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

    const chartHeaders = meta.categoricalHeaders.slice(0, 4);

    const charts = chartHeaders.map((header) => {
      const grouped = ccoGroupCount(filtrados, header);
      const ordered = Object.entries(grouped)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15);

      return {
        header,
        labels: ordered.map((x) => x[0]),
        values: ordered.map((x) => x[1]),
      };
    });

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

    const tableAHeader = chartHeaders[0] || meta.headers[0];
    const tableBHeader = chartHeaders[1] || meta.headers[1] || meta.headers[0];

    const tableA = Object.entries(ccoGroupCount(filtrados, tableAHeader))
      .sort((a, b) => b[1] - a[1])
      .slice(0, 50)
      .map(([nome, total]) => ({ nome, total }));

    const tableB = Object.entries(ccoGroupCount(filtrados, tableBHeader))
      .sort((a, b) => b[1] - a[1])
      .slice(0, 50)
      .map(([nome, total]) => ({ nome, total }));

    return res.json({
      charts,
      porDia: {
        labels: dayLabels,
        values: dayLabels.map((label) => byDay[label]),
      },
      tableAHeader,
      tableBHeader,
      tableA,
      tableB,
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

    const safeHeaders = meta.headers.filter((header) => !ccoIsSecurityAnalystHeader(header));

    const rows = filtrados.slice(start, end).map((row) => {
      const obj = {};
      safeHeaders.forEach((header) => {
        obj[header] = row[header] || "";
      });
      return obj;
    });

    return res.json({
      headers: safeHeaders,
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

// ================== SERVIDOR ==================

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando:");
  console.log(`http://localhost:${PORT}`);
});
