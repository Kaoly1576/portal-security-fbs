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

// ================== ACCESS DASHBOARD ==================

app.get("/access", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "access.html"));
});

async function buscarPlanilhaAccess() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "11b7_2P62T1c1h1gnfYWYUQW2zQSNjteQTBn9jmW9QjU",
    range: "'Solicitações'!A1:AE1000",
  });

  return response.data.values || [];
}

app.get("/api/access-debug", requireAuth, async (req, res) => {
  try {
    const sheets = await conectarSheets();

    const meta = await sheets.spreadsheets.get({
      spreadsheetId: "11b7_2P62T1c1h1gnfYWYUQW2zQSNjteQTBn9jmW9QjU",
    });

    const abas = (meta.data.sheets || []).map(
      (s) => s.properties?.title || "SEM_NOME"
    );

    let values = [];
    let erroLeitura = null;

    try {
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: "11b7_2P62T1c1h1gnfYWYUQW2zQSNjteQTBn9jmW9QjU",
        range: "'Solicitações'!A1:AE20",
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
      spreadsheetId: "11b7_2P62T1c1h1gnfYWYUQW2zQSNjteQTBn9jmW9QjU",
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

app.get("/api/access-dados", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaAccess();

    if (!dados.length) {
      return res.status(500).json({
        ok: false,
        error: "A leitura da planilha não retornou linhas.",
      });
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
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
      details: error.response?.data || null,
    });
  }
});

// ================== SERVIDOR ==================

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando:");
  console.log(`http://localhost:${PORT}`);
});
