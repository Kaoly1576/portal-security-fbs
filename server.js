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

// ================== CONSTANTES ==================

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_CALLBACK_URL =
  process.env.GOOGLE_CALLBACK_URL ||
  "http://localhost:3000/auth/google/callback";

const googleOAuthEnabled =
  Boolean(GOOGLE_CLIENT_ID) && Boolean(GOOGLE_CLIENT_SECRET);

const CADASTRO_SHEET_ID = "1iDkB1uHIIXv7qnVGAWYYPrabl_g1-V_435lYdx66Crc";
const CADASTRO_USUARIOS_RANGE = "usuarios!A1:Z5000";
const CADASTRO_CARGOS_RANGE = "cargos!A1:Z500";
const CADASTRO_NIVEIS_RANGE = "niveis_acesso!A1:Z500";

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
      maxAge: 1000 * 60 * 60 * 12,
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

// ================== CRIAR APROVADOR LEGADO SQLITE ==================

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

// ================== HELPERS GERAIS ==================

function requireAuth(req, res, next) {
  if (!req.session.userId) {
    return res.redirect("/login");
  }
  next();
}

function requireAprovador(req, res, next) {
  if (!req.session.userId) return res.redirect("/login");

  const perfil = String(req.session.perfil || "").toLowerCase();
  const aprovador = String(req.session.aprovador || "0");
  const nivel = String(req.session.nivel_acesso || "").toLowerCase();

  const podeAprovar =
    aprovador === "1" ||
    perfil === "aprovador" ||
    perfil === "master" ||
    perfil === "admin" ||
    nivel === "master" ||
    nivel === "admin";

  if (!podeAprovar) {
    return res.status(403).send("Acesso negado.");
  }

  next();
}

async function conectarSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  const client = await auth.getClient();

  return google.sheets({
    version: "v4",
    auth: client,
  });
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

// ================== HELPERS CADASTRO / PLANILHA ==================

function cadastroNormalize(value) {
  return String(value || "").trim();
}

function cadastroNormalizeLower(value) {
  return cadastroNormalize(value).toLowerCase();
}

function sheetRowsToObjects(rows) {
  if (!rows || !rows.length) return [];
  const headers = rows[0].map((h) => cadastroNormalize(h));

  return rows.slice(1).map((row) => {
    const obj = {};
    headers.forEach((header, index) => {
      obj[header] = row[index] ?? "";
    });
    return obj;
  });
}

function columnToLetter(column) {
  let temp = "";
  let letter = "";
  while (column > 0) {
    temp = (column - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = (column - temp - 1) / 26;
  }
  return letter;
}

async function getCadastroSheetObjects(range, writable = false) {
  const sheets = writable ? await conectarSheetsEdicao() : await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CADASTRO_SHEET_ID,
    range,
  });

  const rows = response.data.values || [];
  return sheetRowsToObjects(rows);
}

async function getUsuariosCadastroSheet() {
  return await getCadastroSheetObjects(CADASTRO_USUARIOS_RANGE, false);
}

async function getCargosCadastroSheet() {
  return await getCadastroSheetObjects(CADASTRO_CARGOS_RANGE, false);
}

async function getNiveisCadastroSheet() {
  return await getCadastroSheetObjects(CADASTRO_NIVEIS_RANGE, false);
}

async function findUsuarioCadastroByEmail(email) {
  const usuarios = await getUsuariosCadastroSheet();
  return (
    usuarios.find(
      (user) => cadastroNormalizeLower(user.email) === cadastroNormalizeLower(email)
    ) || null
  );
}

async function updateUsuarioGoogleInfoByEmail(email, googleProfile = {}) {
  const sheets = await conectarSheetsEdicao();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CADASTRO_SHEET_ID,
    range: CADASTRO_USUARIOS_RANGE,
  });

  const rows = response.data.values || [];
  if (!rows.length) return false;

  const headers = rows[0].map((h) => cadastroNormalize(h));
  const emailCol = headers.findIndex(
    (h) => cadastroNormalizeLower(h) === "email"
  );
  const nomeCol = headers.findIndex(
    (h) => cadastroNormalizeLower(h) === "nome"
  );
  const fotoCol = headers.findIndex(
    (h) => cadastroNormalizeLower(h) === "foto"
  );
  const googleIdCol = headers.findIndex(
    (h) => cadastroNormalizeLower(h) === "google_id"
  );
  const atualizadoCol = headers.findIndex(
    (h) => cadastroNormalizeLower(h) === "atualizado_em"
  );

  if (emailCol === -1) return false;

  const rowIndex = rows.findIndex((row, idx) => {
    if (idx === 0) return false;
    return cadastroNormalizeLower(row[emailCol]) === cadastroNormalizeLower(email);
  });

  if (rowIndex === -1) return false;

  const sheetRowNumber = rowIndex + 1;
  const now = new Date().toISOString();
  const updates = [];

  if (nomeCol !== -1 && googleProfile.nome) {
    updates.push({
      range: `usuarios!${columnToLetter(nomeCol + 1)}${sheetRowNumber}`,
      values: [[googleProfile.nome]],
    });
  }

  if (fotoCol !== -1) {
    updates.push({
      range: `usuarios!${columnToLetter(fotoCol + 1)}${sheetRowNumber}`,
      values: [[googleProfile.foto || ""]],
    });
  }

  if (googleIdCol !== -1) {
    updates.push({
      range: `usuarios!${columnToLetter(googleIdCol + 1)}${sheetRowNumber}`,
      values: [[googleProfile.google_id || ""]],
    });
  }

  if (atualizadoCol !== -1) {
    updates.push({
      range: `usuarios!${columnToLetter(atualizadoCol + 1)}${sheetRowNumber}`,
      values: [[now]],
    });
  }

  if (!updates.length) return true;

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: CADASTRO_SHEET_ID,
    resource: {
      valueInputOption: "USER_ENTERED",
      data: updates,
    },
  });

  return true;
}

async function getUsuariosSheetRaw() {
  const sheets = await conectarSheetsEdicao();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CADASTRO_SHEET_ID,
    range: CADASTRO_USUARIOS_RANGE,
  });

  const rows = response.data.values || [];
  const headers = rows[0] || [];

  return { sheets, rows, headers };
}

function findHeaderIndex(headers, headerName) {
  return headers.findIndex(
    (h) => cadastroNormalizeLower(h) === cadastroNormalizeLower(headerName)
  );
}

// ================== PASSPORT GOOGLE ==================

if (googleOAuthEnabled) {
  passport.use(
    new GoogleStrategy(
      {
        clientID: GOOGLE_CLIENT_ID,
        clientSecret: GOOGLE_CLIENT_SECRET,
        callbackURL: GOOGLE_CALLBACK_URL,
        passReqToCallback: true,
      },
      async (req, accessToken, refreshToken, profile, done) => {
        try {
          const email = profile?.emails?.[0]?.value || "";
          const nome = profile?.displayName || "Usuário";
          const foto = profile?.photos?.[0]?.value || "";
          const googleId = profile?.id || "";
          const isRegisterFlow = req.query.state === "register";

          if (!email || !email.endsWith("@shopee.com")) {
            return done(null, false, {
              message: "Use seu e-mail corporativo @shopee.com.",
            });
          }

          if (isRegisterFlow) {
            return done(null, {
              id: googleId,
              nome,
              email,
              foto,
              google_id: googleId,
              perfil: "pre_cadastro",
              _registerFlow: true,
            });
          }

          const usuarioCadastro = await findUsuarioCadastroByEmail(email);

          if (!usuarioCadastro) {
            return done(null, false, {
              message: "Seu e-mail ainda não possui cadastro no portal.",
            });
          }

          const status = cadastroNormalizeLower(usuarioCadastro.status);
          const cadastroPendente = cadastroNormalize(
            usuarioCadastro.cadastro_pendente
          );

          if (status !== "ativo") {
            return done(null, false, {
              message: "Seu cadastro está inativo ou ainda não foi aprovado.",
            });
          }

          if (cadastroPendente !== "0") {
            return done(null, false, {
              message: "Seu cadastro ainda está pendente de aprovação.",
            });
          }

          await updateUsuarioGoogleInfoByEmail(email, {
            nome,
            foto,
            google_id: googleId,
          });

          const usuarioSessao = {
            id: usuarioCadastro.id || googleId,
            nome: usuarioCadastro.nome || nome,
            email,
            foto: foto || usuarioCadastro.foto || "",
            google_id: googleId,
            perfil: usuarioCadastro.nivel_acesso || "usuario",
            cargo: usuarioCadastro.cargo || "",
            nivel_acesso: usuarioCadastro.nivel_acesso || "",
            area: usuarioCadastro.area || "",
            unidade: usuarioCadastro.unidade || "",
            empresa: usuarioCadastro.empresa || "",
            status: usuarioCadastro.status || "",
            permissoes: usuarioCadastro.permissoes || "",
            aprovador: usuarioCadastro.aprovador || "0",
          };

          return done(null, usuarioSessao);
        } catch (error) {
          console.error("Erro no Google OAuth:", error);
          return done(error, null);
        }
      }
    )
  );

  console.log("Google OAuth habilitado.");
} else {
  console.warn(
    "Google OAuth desabilitado: GOOGLE_CLIENT_ID ou GOOGLE_CLIENT_SECRET ausentes."
  );
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

// ================== LOGIN / CADASTRO / GOOGLE ==================

app.get("/cadastro", (req, res) => {
  if (!req.session?.preCadastroUser) {
    return res.redirect("/auth/google/register");
  }

  return res.sendFile(path.join(__dirname, "public", "cadastro.html"));
});

app.get("/api/login-status", (req, res) => {
  const erro = req.session?.loginError || "";
  req.session.loginError = "";
  return res.json({ erro });
});

app.get("/api/cargos", async (req, res) => {
  try {
    const cargos = await getCargosCadastroSheet();
    const ativos = cargos.filter(
      (item) => cadastroNormalizeLower(item.status) === "ativo"
    );
    return res.json(ativos);
  } catch (err) {
    console.error("Erro ao buscar cargos:", err);
    return res.status(500).json({ erro: "Erro ao buscar cargos" });
  }
});

app.get("/api/niveis-acesso", async (req, res) => {
  try {
    const niveis = await getNiveisCadastroSheet();
    const ativos = niveis.filter(
      (item) => cadastroNormalizeLower(item.status) === "ativo"
    );
    return res.json(ativos);
  } catch (err) {
    console.error("Erro ao buscar níveis de acesso:", err);
    return res.status(500).json({ erro: "Erro ao buscar níveis de acesso" });
  }
});

app.get("/api/pre-cadastro-user", (req, res) => {
  const preUser = req.session?.preCadastroUser || null;
  if (!preUser) {
    return res.status(401).json({ erro: "Usuário de pré-cadastro não autenticado." });
  }
  return res.json(preUser);
});

app.post("/api/usuarios/cadastrar", async (req, res) => {
  try {
    const dados = req.body || {};
    const now = new Date().toISOString();

    if (!cadastroNormalize(dados.nome) || !cadastroNormalize(dados.email)) {
      return res.status(400).json({ erro: "Nome e e-mail são obrigatórios." });
    }

    if (!cadastroNormalize(dados.cargo)) {
      return res.status(400).json({ erro: "Cargo é obrigatório." });
    }

    if (!cadastroNormalize(dados.nivel_acesso)) {
      return res.status(400).json({ erro: "Nível de acesso é obrigatório." });
    }

    const existente = await findUsuarioCadastroByEmail(dados.email);
    if (existente) {
      return res.status(400).json({
        erro: "Já existe um cadastro para este e-mail.",
      });
    }

    const sheets = await conectarSheetsEdicao();
    const novoId = String(Date.now());

    const novaLinha = [
      novoId,                     // id
      dados.nome || "",           // nome
      dados.email || "",          // email
      dados.login || "",          // login
      "",                         // senha
      dados.cargo || "",          // cargo
      dados.area || "",           // area
      "pendente",                 // status
      dados.nivel_acesso || "",   // nivel_acesso
      dados.unidade || "",        // unidade
      dados.google_id || "",      // google_id
      "",                         // permissoes
      0,                          // aprovador
      dados.foto || "",           // foto
      1,                          // cadastro_pendente
      now,                        // criado_em
      dados.empresa || "",        // empresa
      now                         // atualizado_em
    ];

    await sheets.spreadsheets.values.append({
      spreadsheetId: CADASTRO_SHEET_ID,
      range: "usuarios!A1",
      valueInputOption: "USER_ENTERED",
      resource: {
        values: [novaLinha],
      },
    });

    req.session.preCadastroUser = null;

    return res.json({ sucesso: true });
  } catch (err) {
    console.error("Erro ao cadastrar usuário:", err);
    return res.status(500).json({ erro: "Erro ao cadastrar usuário" });
  }
});

app.get("/auth/google", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.status(503).send("Login Google não configurado no servidor.");
  }

  return passport.authenticate("google", {
    scope: ["openid", "profile", "email"],
    session: false,
  })(req, res, next);
});

app.get("/auth/google/register", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.status(503).send("Login Google não configurado no servidor.");
  }

  return passport.authenticate("google", {
    scope: ["openid", "profile", "email"],
    session: false,
    state: "register",
  })(req, res, next);
});

app.get("/auth/google/callback", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.redirect("/login");
  }

  return passport.authenticate("google", { session: false }, (err, user, info) => {
    if (err) {
      console.error("Erro no callback Google:", err);
      req.session.loginError = "Erro ao autenticar com Google.";
      return res.redirect("/login");
    }

    if (!user) {
      req.session.loginError = info?.message || "Acesso não autorizado.";
      return res.redirect("/login");
    }

    if (user._registerFlow) {
      req.session.preCadastroUser = {
        nome: user.nome || "",
        email: user.email || "",
        foto: user.foto || "",
        google_id: user.google_id || "",
      };
      req.session.loginError = "";
      return res.redirect("/cadastro");
    }

    req.session.loginError = "";
    req.session.preCadastroUser = null;
    req.session.userId = user.id;
    req.session.nome = user.nome;
    req.session.email = user.email;
    req.session.perfil = user.perfil;
    req.session.foto = user.foto || "";
    req.session.google_id = user.google_id || "";
    req.session.cargo = user.cargo || "";
    req.session.nivel_acesso = user.nivel_acesso || "";
    req.session.area = user.area || "";
    req.session.unidade = user.unidade || "";
    req.session.empresa = user.empresa || "";
    req.session.permissoes = user.permissoes || "";
    req.session.aprovador = user.aprovador || "0";

    return res.redirect("/portal");
  })(req, res, next);
});

// ================== LOGIN LOCAL LEGADO ==================

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
  return res.sendFile(path.join(__dirname, "public", "av-lost.html"));
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

app.get("/api/me", (req, res) => {
  if (req.session?.userId) {
    return res.json({
      id: req.session.userId,
      nome: req.session.nome || "",
      email: req.session.email || "",
      perfil: req.session.perfil || "",
      foto: req.session.foto || "",
      google_id: req.session.google_id || "",
      cargo: req.session.cargo || "",
      nivel_acesso: req.session.nivel_acesso || "",
      area: req.session.area || "",
      unidade: req.session.unidade || "",
      empresa: req.session.empresa || "",
      permissoes: req.session.permissoes || "",
      aprovador: req.session.aprovador || "0",
    });
  }

  if (req.session?.preCadastroUser) {
    return res.json(req.session.preCadastroUser);
  }

  return res.status(401).json({ erro: "Não autenticado" });
});

// ================== APROVAÇÕES / GESTÃO USUÁRIOS ==================

app.get("/aprovacoes", requireAprovador, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "aprovacoes.html"));
});

app.get("/api/usuarios", requireAprovador, async (req, res) => {
  try {
    const usuarios = await getUsuariosCadastroSheet();

    const ordenados = usuarios.sort((a, b) => {
      const aNome = cadastroNormalize(a.nome).toLowerCase();
      const bNome = cadastroNormalize(b.nome).toLowerCase();
      return aNome.localeCompare(bNome, "pt-BR");
    });

    return res.json(ordenados);
  } catch (err) {
    console.error("Erro ao listar usuários:", err);
    return res.status(500).json({ erro: "Erro ao listar usuários" });
  }
});

app.get("/api/usuarios/pendentes", requireAprovador, async (req, res) => {
  try {
    const usuarios = await getUsuariosCadastroSheet();

    const pendentes = usuarios.filter((u) => {
      const status = cadastroNormalizeLower(u.status);
      const cadastroPendente = cadastroNormalize(u.cadastro_pendente);
      return status === "pendente" || cadastroPendente === "1";
    });

    return res.json(pendentes);
  } catch (err) {
    console.error("Erro ao listar pendentes:", err);
    return res.status(500).json({ erro: "Erro ao listar pendentes" });
  }
});

app.post("/api/usuarios/aprovar/:id", requireAprovador, async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) {
      return res.status(400).json({ erro: "ID inválido." });
    }

    const { sheets, rows, headers } = await getUsuariosSheetRaw();

    if (!rows.length) {
      return res.status(404).json({ erro: "Planilha vazia." });
    }

    const idCol = findHeaderIndex(headers, "id");
    const statusCol = findHeaderIndex(headers, "status");
    const cadastroPendenteCol = findHeaderIndex(headers, "cadastro_pendente");
    const atualizadoCol = findHeaderIndex(headers, "atualizado_em");

    if (idCol === -1 || statusCol === -1 || cadastroPendenteCol === -1) {
      return res.status(500).json({
        erro: "Cabeçalhos obrigatórios não encontrados na planilha.",
      });
    }

    const rowIndex = rows.findIndex((row, idx) => {
      if (idx === 0) return false;
      return cadastroNormalize(row[idCol]) === id;
    });

    if (rowIndex === -1) {
      return res.status(404).json({ erro: "Usuário não encontrado." });
    }

    const sheetRowNumber = rowIndex + 1;
    const updates = [
      {
        range: `usuarios!${columnToLetter(statusCol + 1)}${sheetRowNumber}`,
        values: [["ativo"]],
      },
      {
        range: `usuarios!${columnToLetter(cadastroPendenteCol + 1)}${sheetRowNumber}`,
        values: [["0"]],
      },
    ];

    if (atualizadoCol !== -1) {
      updates.push({
        range: `usuarios!${columnToLetter(atualizadoCol + 1)}${sheetRowNumber}`,
        values: [[new Date().toISOString()]],
      });
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: CADASTRO_SHEET_ID,
      resource: {
        valueInputOption: "USER_ENTERED",
        data: updates,
      },
    });

    return res.json({ sucesso: true });
  } catch (err) {
    console.error("Erro ao aprovar usuário:", err);
    return res.status(500).json({ erro: "Erro ao aprovar usuário" });
  }
});

app.put("/api/usuarios/:id", requireAprovador, async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    const dados = req.body || {};

    if (!id) {
      return res.status(400).json({ erro: "ID inválido." });
    }

    const { sheets, rows, headers } = await getUsuariosSheetRaw();

    if (!rows.length) {
      return res.status(404).json({ erro: "Planilha vazia." });
    }

    const idCol = findHeaderIndex(headers, "id");
    if (idCol === -1) {
      return res.status(500).json({ erro: "Coluna id não encontrada." });
    }

    const rowIndex = rows.findIndex((row, idx) => {
      if (idx === 0) return false;
      return cadastroNormalize(row[idCol]) === id;
    });

    if (rowIndex === -1) {
      return res.status(404).json({ erro: "Usuário não encontrado." });
    }

    const sheetRowNumber = rowIndex + 1;

    const editableFields = [
      "cargo",
      "nivel_acesso",
      "area",
      "unidade",
      "empresa",
      "status",
      "aprovador",
      "permissoes",
      "cadastro_pendente",
    ];

    const updates = [];

    editableFields.forEach((field) => {
      const col = findHeaderIndex(headers, field);
      if (col !== -1 && Object.prototype.hasOwnProperty.call(dados, field)) {
        updates.push({
          range: `usuarios!${columnToLetter(col + 1)}${sheetRowNumber}`,
          values: [[dados[field] ?? ""]],
        });
      }
    });

    const atualizadoCol = findHeaderIndex(headers, "atualizado_em");
    if (atualizadoCol !== -1) {
      updates.push({
        range: `usuarios!${columnToLetter(atualizadoCol + 1)}${sheetRowNumber}`,
        values: [[new Date().toISOString()]],
      });
    }

    if (!updates.length) {
      return res.status(400).json({ erro: "Nenhum campo válido para atualizar." });
    }

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: CADASTRO_SHEET_ID,
      resource: {
        valueInputOption: "USER_ENTERED",
        data: updates,
      },
    });

    return res.json({ sucesso: true });
  } catch (err) {
    console.error("Erro ao atualizar usuário:", err);
    return res.status(500).json({ erro: "Erro ao atualizar usuário" });
  }
});

// ================== GOOGLE SHEETS DASHBOARD AV ==================

// ================== GOOGLE SHEETS DASHBOARD AV ==================

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

// ================== DESLIGADOS DASHBOARD ==================

app.get("/desligados", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "desligados.html"));
});

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
  return dNormalize(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function parseDateBRDesligados(value) {
  const str = dNormalize(value);
  if (!str) return null;

  const match = str.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (!match) return null;

  const day = Number(match[1]);
  const month = Number(match[2]);
  let year = Number(match[3]);

  if (year < 1000) year += 2000;
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

  return linhas.map((linha) => {
    const obj = {};

    headers.forEach((header, index) => {
      obj[header] = linha[index] ?? "";
    });

    obj._data = parseDateBRDesligados(obj[DESLIGADOS_COLUMNS.data]);
    obj._mesRef = obj._data
      ? `${obj._data.getFullYear()}-${String(obj._data.getMonth() + 1).padStart(2, "0")}`
      : "";
    obj._diaRef = obj._data
      ? String(obj._data.getDate()).padStart(2, "0")
      : "";

    return obj;
  });
}

async function carregarDesligadosComCache() {
  const now = Date.now();

  if (desligadosCache && now - desligadosCacheTime < DESLIGADOS_CACHE_TTL) {
    return desligadosCache;
  }

  desligadosCache = await carregarDesligadosRaw();
  desligadosCacheTime = now;

  console.log("DESLIGADOS cache atualizado:", desligadosCache.length);

  return desligadosCache;
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

    const agora = new Date();
    agora.setHours(agora.getHours() - 3);

    return res.json({
      total,
      enviadosSim,
      bloqueadosSim,
      pendentes,
      empresas,
      unidades,
      taxaBloqueio,
      ultimaAtualizacao: agora.toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
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

    // Top motivos Security:
// filtra apenas os registros cujo Motivo de desligamento seja SECURITY
// e então agrupa pelos valores de Controle interno
const topMotivosSecurityMap = {};

filtrados.forEach((row) => {
  const motivo = dNormalize(row[DESLIGADOS_COLUMNS.motivo]);
  const motivoLower = dNormalizeLower(motivo);

  // só entra se o motivo de desligamento for SECURITY
  if (motivoLower !== "security") return;

  const controle = dNormalize(row[DESLIGADOS_COLUMNS.controle]);
  const controleLower = dNormalizeLower(controle);

  // ignora lixo / vazios
  if (!controle) return;
  if (controleLower === "null") return;
  if (controleLower === "undefined") return;
  if (controleLower === "sem valor") return;
  if (controleLower === "-") return;
  if (controleLower === "controle interno") return;
  if (controleLower === "security") return;

  topMotivosSecurityMap[controle] =
    (topMotivosSecurityMap[controle] || 0) + 1;
});

const topMotivosSecurity = Object.entries(topMotivosSecurityMap)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 12);

    const dayMap = {};
    filtrados.forEach((row) => {
      if (!row._diaRef) return;
      dayMap[row._diaRef] = (dayMap[row._diaRef] || 0) + 1;
    });

    const diaLabels = Array.from({ length: 31 }, (_, i) =>
      String(i + 1).padStart(2, "0")
    );

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

      topMotivosSecurity: {
        labels: topMotivosSecurity.map(([nome]) => nome),
        values: topMotivosSecurity.map(([, total]) => total),
      },

      porDia: {
        labels: diaLabels,
        values: diaLabels.map((label) => dayMap[label] || 0),
      },

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
const CCO_FBS_SHEET_NAME = "Solicitações de imagens";

let ccoCache = null;
let ccoCacheTime = 0;
let ccoSheetTitle = null;
const CCO_CACHE_TTL = 5 * 60 * 1000;

function ccoNormalize(value) {
  return String(value || "").trim();
}

function ccoNormalizeLower(value) {
  return ccoNormalize(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
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

  for (const possibility of possibilities) {
    const found = headers.find((h) => ccoNormalizeLower(h).includes(ccoNormalizeLower(possibility)));
    if (found) return found;
  }

  return null;
}

async function ccoGetBestSheetTitle() {
  if (ccoSheetTitle) return ccoSheetTitle;
  ccoSheetTitle = CCO_FBS_SHEET_NAME;
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
    "Email do solicitante",
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
          filtrados
            .map((r) => ccoNormalize(r[meta.unidadeHeader]))
            .filter(Boolean)
        ).size
      : 0;

    const statusMap = meta.statusHeader
      ? ccoGroupCount(filtrados, meta.statusHeader)
      : {};

    const topStatus = Object.entries(statusMap).sort((a, b) => b[1] - a[1])[0] || null;

    const headers = meta.headers;

    function findHeader(names) {
      return headers.find((h) =>
        names.some((n) => ccoNormalizeLower(h).includes(ccoNormalizeLower(n)))
      );
    }

    const ocorrenciaHeader = findHeader(["ocorrencia", "ocorrência"]);
    const solicitanteHeader = findHeader([
      "email do solicitante",
      "e-mail do solicitante",
      "solicitante",
      "requisitante"
    ]);

    const totalOcorrencias = ocorrenciaHeader
      ? filtrados.filter((row) => ccoNormalize(row[ocorrenciaHeader])).length
      : 0;

    let solicitanteLider = "-";
    if (solicitanteHeader) {
      const solicitanteMap = ccoGroupCount(
        filtrados.filter((row) => ccoNormalize(row[solicitanteHeader])),
        solicitanteHeader
      );

      const topSolicitante =
        Object.entries(solicitanteMap).sort((a, b) => b[1] - a[1])[0] || null;

      if (topSolicitante) {
        solicitanteLider = `${topSolicitante[0]} (${topSolicitante[1]})`;
      }
    }

    return res.json({
      total,
      totalUnidades,
      totalOcorrencias,
      solicitanteLider,
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

    const headers = meta.headers;

    function findHeader(names) {
      return headers.find((h) =>
        names.some((n) => ccoNormalizeLower(h).includes(ccoNormalizeLower(n)))
      );
    }

    const tipoHeader = findHeader(["tipo de solicitacao", "tipo de solicitação", "tipo"]);
    const ocorrenciaHeader = findHeader(["ocorrencia", "ocorrência"]);
    const unidadeHeader = findHeader(["unidade", "site", "base"]);
    const setorHeader = findHeader(["setor"]);
    const statusHeader = findHeader(["status"]);
    const solicitanteHeader = findHeader([
      "email do solicitante",
      "e-mail do solicitante",
      "solicitante",
      "requisitante"
    ]);

    function buildTable(header, onlyFilled = false) {
      if (!header) return [];

      let base = filtrados;
      if (onlyFilled) {
        base = filtrados.filter((row) => ccoNormalize(row[header]));
      }

      return Object.entries(ccoGroupCount(base, header))
        .sort((a, b) => b[1] - a[1])
        .slice(0, 50)
        .map(([nome, total]) => ({ nome, total }));
    }

    const tableTipo = buildTable(tipoHeader, true);
    const tableOcorrencia = buildTable(ocorrenciaHeader, true);
    const tableUnidade = buildTable(unidadeHeader, true);
    const tableSetor = buildTable(setorHeader, true);
    const tableStatus = buildTable(statusHeader, true);

    const solicitanteGrouped = solicitanteHeader
      ? Object.entries(
          ccoGroupCount(
            filtrados.filter((row) => ccoNormalize(row[solicitanteHeader])),
            solicitanteHeader
          )
        )
          .sort((a, b) => b[1] - a[1])
          .slice(0, 15)
      : [];

    const chartSolicitante = {
      labels: solicitanteGrouped.map((x) => x[0]),
      values: solicitanteGrouped.map((x) => x[1]),
    };

    const byDay = {};
    filtrados.forEach((row) => {
      if (!row._primaryDate) return;

      const d = row._primaryDate;
      const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
      byDay[key] = (byDay[key] || 0) + 1;
    });

    const dayLabels = Object.keys(byDay).sort((a, b) => a.localeCompare(b));

    return res.json({
      tableTipo,
      tableOcorrencia,
      tableUnidade,
      tableSetor,
      tableStatus,
      chartSolicitante,
      porDia: {
        labels: dayLabels,
        values: dayLabels.map((d) => byDay[d]),
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
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 500);

    const start = (page - 1) * limit;
    const end = start + limit;

    const preferredHeaders = meta.headers.filter((header) => !ccoIsSecurityHeader(header));

    const rows = filtrados.slice(start, end).map((row) => {
      const obj = {};
      preferredHeaders.forEach((header) => {
        obj[header] = row[header] || "";
      });
      return obj;
    });

    return res.json({
      headers: preferredHeaders,
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
  return fmNorm(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
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

function fmMonthLabel(monthIndex) {
  const meses = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
  ];
  return meses[monthIndex] || "";
}

function fmParseMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => fmNorm(v))
    .filter(Boolean);
}

function fmMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(fmNorm(fieldValue));
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

  const agencies = fmParseMulti(query.agency);
  const regionals = fmParseMulti(query.regional);
  const cities = fmParseMulti(query.city);
  const vehicles = fmParseMulti(query.vehicle);
  const monitors = fmParseMulti(query.monitor);

  return rows.filter((row) => {
    if (!row._date) return false;

    const iso = fmFormatISO(row._date);
    if (dataInicio && iso < dataInicio) return false;
    if (dataFim && iso > dataFim) return false;

    if (!fmMatchesMulti(row["Agency"], agencies)) return false;
    if (!fmMatchesMulti(row["Regional"], regionals)) return false;
    if (!fmMatchesMulti(row["City"], cities)) return false;
    if (!fmMatchesMulti(row["Vehicle"], vehicles)) return false;
    if (!fmMatchesMulti(row["NOME DO MONITOR"], monitors)) return false;

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
        row["Validação Ocorrência"],
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
    obj._isoDate = obj._date ? fmFormatISO(obj._date) : "";
    obj._day = obj._date ? obj._date.getDate() : null;
    obj._month = obj._date ? obj._date.getMonth() : null;
    obj._year = obj._date ? obj._date.getFullYear() : null;

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

    const agencies = [...new Set(rows.map((r) => fmNorm(r["Agency"])).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const regionals = [...new Set(rows.map((r) => fmNorm(r["Regional"])).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const cities = [...new Set(rows.map((r) => fmNorm(r["City"])).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const vehicles = [...new Set(rows.map((r) => fmNorm(r["Vehicle"])).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const monitors = [...new Set(rows.map((r) => fmNorm(r["NOME DO MONITOR"])).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, "pt-BR"));

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
      if (!row._day) return;
      const day = String(row._day).padStart(2, "0");
      byDay[day] = (byDay[day] || 0) + 1;
    });

    const dayLabels = Array.from({ length: 31 }, (_, i) =>
      String(i + 1).padStart(2, "0")
    );

    const byMonth = {};
    filtrados.forEach((row) => {
      if (row._month === null || row._month === undefined) return;
      byMonth[row._month] = (byMonth[row._month] || 0) + 1;
    });

    const monthIndexes = Array.from({ length: 12 }, (_, i) => i);

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
        values: dayLabels.map((d) => byDay[d] || 0),
      },
      porMes: {
        labels: monthIndexes.map((m) => fmMonthLabel(m)),
        values: monthIndexes.map((m) => byMonth[m] || 0),
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


// ================== RONDAS DASHBOARD ==================

app.get("/rondas", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "rondas.html"));
});

const RONDAS_SHEET_ID = "1jFF45tBHXerhWWXC-X5fHgVyeUxKvx7Q0MiIwx8mUjU";

let rondasCache = null;
let rondasCacheTime = 0;
const RONDAS_CACHE_TTL = 5 * 60 * 1000;

function rondaNorm(value) {
  return String(value || "").trim();
}

function rondaNormUpper(value) {
  return rondaNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function rondaNormLower(value) {
  return rondaNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function rondaParseDate(value) {
  if (!value) return null;
  const raw = String(value).trim();

  let m = raw.match(
    /^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m) {
    const [, dd, mm, yyyy, hh = "00", mi = "00", ss = "00"] = m;
    const d = new Date(`${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  m = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) {
    const d = new Date(`${raw}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const fallback = new Date(raw);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function rondaFormatDateBR(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  const day = String(date.getDate()).padStart(2, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const year = date.getFullYear();
  return `${day}/${month}/${year}`;
}

function rondaFormatISO(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function rondaSplitMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => rondaNorm(v))
    .filter(Boolean);
}

function rondaMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(rondaNorm(fieldValue));
}

function rondaIncludesText(source, term) {
  return rondaNormUpper(source).includes(rondaNormUpper(term));
}

function rondaSortUnique(arr) {
  return [...new Set(arr.filter(Boolean))].sort((a, b) =>
    String(a).localeCompare(String(b), "pt-BR")
  );
}

function rondaPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);

  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;

  return ((current - previous) / previous) * 100;
}

function mapRondaGeralRow(row) {
  return {
    data: rondaNorm(row[0]),
    dataObj: rondaParseDate(row[0]),
    unidade: rondaNorm(row[1]),
    plantao: rondaNorm(row[2]),
    nome: rondaNorm(row[3]),
    acao: rondaNorm(row[4]),
    tipo_ronda: rondaNorm(row[5]) || "Ronda Geral",
    origem_ronda: "Ronda Geral",
  };
}

function mapPortasRow(row) {
  return {
    data: rondaNorm(row[0]),
    dataObj: rondaParseDate(row[0]),
    unidade: rondaNorm(row[1]),
    plantao: rondaNorm(row[2]),
    nome: rondaNorm(row[3]),
    acao: rondaNorm(row[4]),
    tipo_ronda: rondaNorm(row[5]) || "Portas de emergência",
    origem_ronda: "Portas de emergência",
  };
}

function mapEstoqueRow(row) {
  return {
    data: rondaNorm(row[0]),
    dataObj: rondaParseDate(row[0]),
    unidade: rondaNorm(row[1]),
    plantao: rondaNorm(row[2]),
    nome: rondaNorm(row[3]),
    acao: "Ronda",
    tipo_ronda: rondaNorm(row[4]) || "Estoque",
    origem_ronda: "Ronda Estoque",
  };
}

async function loadRondasRaw() {
  const sheets = await conectarSheets();
  const all = [];

  const configs = [
    {
      aba: "Ronda Geral",
      range: "'Ronda Geral'!B:G",
      mapper: mapRondaGeralRow,
    },
    {
      aba: "Ronda Portas de emergência",
      range: "'Ronda Portas de emergência'!C:H",
      mapper: mapPortasRow,
    },
    {
      aba: "Ronda estoque RK",
      range: "'Ronda estoque RK'!B:F",
      mapper: mapEstoqueRow,
    },
  ];

  for (const cfg of configs) {
    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: RONDAS_SHEET_ID,
      range: cfg.range,
    });

    const rows = resp?.data?.values || [];
    if (!rows.length) continue;

    const dataRows = rows.slice(1);

    for (const row of dataRows) {
      if (!row || row.every((cell) => !String(cell || "").trim())) continue;

      const record = cfg.mapper(row);

      if (
        !record.data &&
        !record.unidade &&
        !record.nome &&
        !record.tipo_ronda
      ) {
        continue;
      }

      all.push(record);
    }
  }

  return all;
}

async function loadRondasComCache() {
  const now = Date.now();

  if (rondasCache && now - rondasCacheTime < RONDAS_CACHE_TTL) {
    return rondasCache;
  }

  rondasCache = await loadRondasRaw();
  rondasCacheTime = now;

  console.log("RONDAS cache atualizado:", rondasCache.length);

  return rondasCache;
}

function filterRondas(records, query) {
  const dataInicial = rondaNorm(query.dataInicial);
  const dataFinal = rondaNorm(query.dataFinal);

  const origens = rondaSplitMulti(query.origem);
  const unidades = rondaSplitMulti(query.unidade);
  const plantoes = rondaSplitMulti(query.plantao);
  const tipos = rondaSplitMulti(query.tipo);
  const nomes = rondaSplitMulti(query.nome);
  const busca = rondaNormLower(query.busca);

  return records.filter((r) => {
    const iso = r.dataObj ? rondaFormatISO(r.dataObj) : "";

    if (dataInicial && (!iso || iso < dataInicial)) return false;
    if (dataFinal && (!iso || iso > dataFinal)) return false;

    if (!rondaMatchesMulti(r.origem_ronda, origens)) return false;
    if (!rondaMatchesMulti(r.unidade, unidades)) return false;
    if (!rondaMatchesMulti(r.plantao, plantoes)) return false;
    if (!rondaMatchesMulti(r.tipo_ronda, tipos)) return false;
    if (!rondaMatchesMulti(r.nome, nomes)) return false;

    if (busca) {
      const combined = [
        r.data,
        r.origem_ronda,
        r.unidade,
        r.plantao,
        r.nome,
        r.acao,
        r.tipo_ronda,
      ].join(" ");

      if (!rondaIncludesText(combined, busca)) return false;
    }

    return true;
  });
}

function buildRondasResumo(filtered) {
  const totalRondas = filtered.length;

  const validDates = filtered
    .map((r) => r.dataObj)
    .filter(Boolean)
    .sort((a, b) => a - b);

  let rondasMesAtual = 0;
  let rondasMesAnterior = 0;

  if (validDates.length) {
    const latest = validDates[validDates.length - 1];
    const currentMonth = latest.getMonth();
    const currentYear = latest.getFullYear();

    const prevRef = new Date(currentYear, currentMonth - 1, 1);
    const prevMonth = prevRef.getMonth();
    const prevYear = prevRef.getFullYear();

    rondasMesAtual = filtered.filter(
      (r) =>
        r.dataObj &&
        r.dataObj.getMonth() === currentMonth &&
        r.dataObj.getFullYear() === currentYear
    ).length;

    rondasMesAnterior = filtered.filter(
      (r) =>
        r.dataObj &&
        r.dataObj.getMonth() === prevMonth &&
        r.dataObj.getFullYear() === prevYear
    ).length;
  }

  const colaboradoresUnicos = new Set(
    filtered.map((r) => rondaNorm(r.nome)).filter(Boolean)
  ).size;

  const unidadesUnicas = new Set(
    filtered.map((r) => rondaNorm(r.unidade)).filter(Boolean)
  ).size;

  return {
    totalRondas,
    rondasMesAtual,
    rondasMesAnterior,
    variacaoMensal: rondaPercentChange(rondasMesAtual, rondasMesAnterior),
    colaboradoresUnicos,
    unidadesUnicas,
  };
}

function buildRondasGraficos(filtered) {
  const dias = Array.from({ length: 31 }, (_, i) =>
    String(i + 1).padStart(2, "0")
  );
  const porDiaMap = Object.fromEntries(dias.map((d) => [d, 0]));

  filtered.forEach((r) => {
    if (!r.dataObj) return;
    const dia = String(r.dataObj.getDate()).padStart(2, "0");
    porDiaMap[dia] = (porDiaMap[dia] || 0) + 1;
  });

  const porDia = {
    labels: dias,
    valores: dias.map((d) => porDiaMap[d] || 0),
    metas: dias.map(() => 35),
  };

  let mesAtual = 0;
  let mesAnterior = 0;

  const validDates = filtered
    .map((r) => r.dataObj)
    .filter(Boolean)
    .sort((a, b) => a - b);

  if (validDates.length) {
    const latest = validDates[validDates.length - 1];
    const currMonth = latest.getMonth();
    const currYear = latest.getFullYear();

    const prevRef = new Date(currYear, currMonth - 1, 1);
    const prevMonth = prevRef.getMonth();
    const prevYear = prevRef.getFullYear();

    mesAtual = filtered.filter(
      (r) =>
        r.dataObj &&
        r.dataObj.getMonth() === currMonth &&
        r.dataObj.getFullYear() === currYear
    ).length;

    mesAnterior = filtered.filter(
      (r) =>
        r.dataObj &&
        r.dataObj.getMonth() === prevMonth &&
        r.dataObj.getFullYear() === prevYear
    ).length;
  }

  function buildTop(records, key, meta, limit = 10) {
    const map = {};

    records.forEach((r) => {
      const k = rondaNorm(r[key]) || "NÃO INFORMADO";
      map[k] = (map[k] || 0) + 1;
    });

    const entries = Object.entries(map)
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit);

    return {
      labels: entries.map(([k]) => k),
      valores: entries.map(([, v]) => v),
      metas: entries.map(() => meta),
    };
  }

  return {
    porDia,
    comparativoMensal: {
      mesAtual,
      mesAnterior,
      metaMensal: 1000,
    },
    porUnidade: buildTop(filtered, "unidade", 150),
    porPlantao: buildTop(filtered, "plantao", 200),
    porTipo: buildTop(filtered, "tipo_ronda", 300),
  };
}

function buildRondasFiltros(filteredBase) {
  return {
    origens: rondaSortUnique(filteredBase.map((r) => r.origem_ronda)),
    unidades: rondaSortUnique(filteredBase.map((r) => r.unidade)),
    plantoes: rondaSortUnique(filteredBase.map((r) => r.plantao)),
    tipos: rondaSortUnique(filteredBase.map((r) => r.tipo_ronda)),
    nomes: rondaSortUnique(filteredBase.map((r) => r.nome)),
  };
}

function buildRondasDetalhes(filtered, page = 1, limit = 20) {
  const sorted = [...filtered].sort((a, b) => {
    const ad = a.dataObj ? a.dataObj.getTime() : 0;
    const bd = b.dataObj ? b.dataObj.getTime() : 0;
    return bd - ad;
  });

  const total = sorted.length;
  const totalPages = Math.max(1, Math.ceil(total / limit));
  const currentPage = Math.min(Math.max(1, page), totalPages);
  const start = (currentPage - 1) * limit;

  const items = sorted.slice(start, start + limit).map((r) => ({
    data: r.dataObj ? rondaFormatDateBR(r.dataObj) : r.data,
    origem_ronda: r.origem_ronda,
    unidade: r.unidade,
    plantao: r.plantao,
    nome: r.nome,
    acao: r.acao,
    tipo_ronda: r.tipo_ronda,
  }));

  return {
    page: currentPage,
    limit,
    total,
    totalPages,
    items,
  };
}

app.get("/api/rondas-debug", requireAuth, async (req, res) => {
  try {
    const all = await loadRondasComCache();
    res.json({
      total: all.length,
      amostra: all.slice(0, 5),
    });
  } catch (error) {
    console.error("Erro /api/rondas-debug:", error);
    res.status(500).json({
      error: "Erro no debug de rondas.",
      detalhe: error.message,
      stack: error.stack,
    });
  }
});

app.get("/api/rondas-filtros", requireAuth, async (req, res) => {
  try {
    const all = await loadRondasComCache();
    const filtered = filterRondas(all, req.query);
    res.json(buildRondasFiltros(filtered));
  } catch (error) {
    console.error("Erro /api/rondas-filtros:", error);
    res.status(500).json({
      error: "Erro ao carregar filtros de rondas.",
      detalhe: error.message,
    });
  }
});

app.get("/api/rondas-resumo", requireAuth, async (req, res) => {
  try {
    const all = await loadRondasComCache();
    const filtered = filterRondas(all, req.query);
    res.json(buildRondasResumo(filtered));
  } catch (error) {
    console.error("Erro /api/rondas-resumo:", error);
    res.status(500).json({
      error: "Erro ao carregar resumo de rondas.",
      detalhe: error.message,
      stack: error.stack,
    });
  }
});

app.get("/api/rondas-graficos", requireAuth, async (req, res) => {
  try {
    const all = await loadRondasComCache();
    const filtered = filterRondas(all, req.query);
    res.json(buildRondasGraficos(filtered));
  } catch (error) {
    console.error("Erro /api/rondas-graficos:", error);
    res.status(500).json({
      error: "Erro ao carregar gráficos de rondas.",
      detalhe: error.message,
    });
  }
});

app.get("/api/rondas-detalhes", requireAuth, async (req, res) => {
  try {
    const all = await loadRondasComCache();
    const filtered = filterRondas(all, req.query);

    const page = Number(req.query.page || 1);
    const limit = Number(req.query.limit || 20);

    res.json(buildRondasDetalhes(filtered, page, limit));
  } catch (error) {
    console.error("Erro /api/rondas-detalhes:", error);
    res.status(500).json({
      error: "Erro ao carregar detalhes de rondas.",
      detalhe: error.message,
    });
  }
});

// ================== REGISTRO DE LACRES DASHBOARD ==================

app.get("/registro-lacres", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registro-lacres.html"));
});

const LACRES_SPREADSHEET_ID = "1kxyx3nSpltdddSQkEGHHjOwcEtHMBd3RuQdk-w9q218";
// ajuste o nome da aba se necessário
const LACRES_RANGE = "'Registro de lacres'!A1:R200000";

let lacresCache = null;
let lacresCacheTime = 0;
const LACRES_CACHE_TTL = 5 * 60 * 1000;

function lacreNorm(value) {
  return String(value || "").trim();
}

function lacreNormLower(value) {
  return lacreNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function lacreParseDate(value) {
  const str = lacreNorm(value);
  if (!str) return null;

  let m = str.match(
    /^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m) {
    const [, dd, mm, yyyy, hh = "00", mi = "00", ss = "00"] = m;
    const d = new Date(`${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const d = new Date(`${str}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const fallback = new Date(str);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function lacreFormatISO(date) {
  if (!date) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function lacreFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function lacreSplitMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => lacreNorm(v))
    .filter(Boolean);
}

function lacreMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(lacreNorm(fieldValue));
}

function lacreUnique(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "pt-BR")
  );
}

function lacrePercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);
  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;
  return ((current - previous) / previous) * 100;
}

function lacreGroupCount(rows, field) {
  const map = {};
  rows.forEach((row) => {
    const key = lacreNorm(row[field]) || "NÃO INFORMADO";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

async function carregarLacresRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: LACRES_SPREADSHEET_ID,
    range: LACRES_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const headers = values[0].map((h, i) => lacreNorm(h) || `COL_${i + 1}`);
  const linhas = values.slice(1);

  const rows = linhas
    .map((linha) => {
      const obj = {};
      headers.forEach((header, index) => {
        obj[header] = linha[index] ?? "";
      });

      obj._dataObj = lacreParseDate(obj["DATA"]);
      obj._isoDate = obj._dataObj ? lacreFormatISO(obj._dataObj) : "";
      obj._dia = obj._dataObj ? String(obj._dataObj.getDate()).padStart(2, "0") : "";
      obj._mes = obj._dataObj
        ? `${obj._dataObj.getFullYear()}-${String(obj._dataObj.getMonth() + 1).padStart(2, "0")}`
        : "";

      return obj;
    })
    .filter((row) => {
      return (
        lacreNorm(row["DATA"]) ||
        lacreNorm(row["UNIDADE"]) ||
        lacreNorm(row["NOME DO VIGILANTE "]) ||
        lacreNorm(row["PLACA"])
      );
    });

  return rows;
}

async function carregarLacresComCache() {
  const now = Date.now();

  if (lacresCache && now - lacresCacheTime < LACRES_CACHE_TTL) {
    return lacresCache;
  }

  lacresCache = await carregarLacresRaw();
  lacresCacheTime = now;

  console.log("LACRES cache atualizado:", lacresCache.length);

  return lacresCache;
}

function filtrarLacres(rows, query) {
  const dataInicio = lacreNorm(query.dataInicio);
  const dataFim = lacreNorm(query.dataFim);

  const unidades = lacreSplitMulti(query.unidade);
  const tiposCarga = lacreSplitMulti(query.tipoCarga);
  const origens = lacreSplitMulti(query.origem);
  const destinos = lacreSplitMulti(query.destino);
  const vigilantes = lacreSplitMulti(query.vigilante);
  const motoristas = lacreSplitMulti(query.motorista);
  const placas = lacreSplitMulti(query.placa);
  const statusEnvio = lacreSplitMulti(query.statusEnvio);
  const lacreCorreto = lacreSplitMulti(query.lacreCorreto);

  const busca = lacreNormLower(query.busca);

  return rows.filter((row) => {
    if (dataInicio && (!row._isoDate || row._isoDate < dataInicio)) return false;
    if (dataFim && (!row._isoDate || row._isoDate > dataFim)) return false;

    if (!lacreMatchesMulti(row["UNIDADE"], unidades)) return false;
    if (!lacreMatchesMulti(row["TIPO DE CARGA"], tiposCarga)) return false;
    if (!lacreMatchesMulti(row["ORIGEM"], origens)) return false;
    if (!lacreMatchesMulti(row["DESTINO"], destinos)) return false;
    if (!lacreMatchesMulti(row["NOME DO VIGILANTE "], vigilantes)) return false;
    if (!lacreMatchesMulti(row["NOME DO MOTORISTA"], motoristas)) return false;
    if (!lacreMatchesMulti(row["PLACA"], placas)) return false;
    if (!lacreMatchesMulti(row["STATUS DE ENVIO"], statusEnvio)) return false;
    if (!lacreMatchesMulti(String(row["O LACRE ESTÁ CORRETO?"]), lacreCorreto)) return false;

    if (busca) {
      const text = [
        row["UNIDADE"],
        row["NOME DO VIGILANTE "],
        row["TIPO DE CARGA"],
        row["ORIGEM"],
        row["NOME DO MOTORISTA"],
        row["PLACA"],
        row["N° LACRE BAÚ"],
        row["N° LACRE LATERAL"],
        row["DESTINO"],
        row["STATUS DE ENVIO"],
        row["O LACRE ESTÁ CORRETO?"],
        row["CONFIRME O NÚMERO DO LACRE"],
      ]
        .map(lacreNormLower)
        .join(" ");

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

app.get("/api/registro-lacres-filtros", requireAuth, async (req, res) => {
  try {
    const dados = await carregarLacresComCache();

    return res.json({
      unidades: lacreUnique(dados.map((r) => lacreNorm(r["UNIDADE"]))),
      tiposCarga: lacreUnique(dados.map((r) => lacreNorm(r["TIPO DE CARGA"]))),
      origens: lacreUnique(dados.map((r) => lacreNorm(r["ORIGEM"]))),
      destinos: lacreUnique(dados.map((r) => lacreNorm(r["DESTINO"]))),
      vigilantes: lacreUnique(dados.map((r) => lacreNorm(r["NOME DO VIGILANTE "]))),
      motoristas: lacreUnique(dados.map((r) => lacreNorm(r["NOME DO MOTORISTA"]))),
      placas: lacreUnique(dados.map((r) => lacreNorm(r["PLACA"]))),
      statusEnvio: lacreUnique(dados.map((r) => lacreNorm(r["STATUS DE ENVIO"]))),
      lacreCorreto: lacreUnique(dados.map((r) => lacreNorm(String(r["O LACRE ESTÁ CORRETO?"])))),
    });
  } catch (error) {
    console.error("Erro /api/registro-lacres-filtros:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/registro-lacres-resumo", requireAuth, async (req, res) => {
  try {
    const dados = await carregarLacresComCache();
    const filtrados = filtrarLacres(dados, req.query);

    const total = filtrados.length;

    // base para descobrir o mês atual a partir do filtro aplicado
    const validDates = filtrados
      .map((r) => r._dataObj)
      .filter(Boolean)
      .sort((a, b) => a - b);

    let mesAtual = 0;
    let mesAnterior = 0;

    if (validDates.length) {
      const latest = validDates[validDates.length - 1];
      const currMonth = latest.getMonth();
      const currYear = latest.getFullYear();

      const prevRef = new Date(currYear, currMonth - 1, 1);
      const prevMonth = prevRef.getMonth();
      const prevYear = prevRef.getFullYear();

      // mês atual: respeita todos os filtros
      mesAtual = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === currMonth &&
          r._dataObj.getFullYear() === currYear
      ).length;

      // mês anterior: ignora apenas o filtro de data, mas respeita os demais
      const querySemData = { ...req.query };
      delete querySemData.dataInicio;
      delete querySemData.dataFim;

      const baseComparativo = filtrarLacres(dados, querySemData);

      mesAnterior = baseComparativo.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === prevMonth &&
          r._dataObj.getFullYear() === prevYear
      ).length;
    }

    const corretos = filtrados.filter(
      (r) => lacreNormLower(String(r["O LACRE ESTÁ CORRETO?"])) === "true"
    ).length;

    const incorretos = filtrados.filter((r) => {
      const v = lacreNormLower(String(r["O LACRE ESTÁ CORRETO?"]));
      return v && v !== "true";
    }).length;

    return res.json({
      total,
      mesAtual,
      mesAnterior,
      variacaoMensal: lacrePercentChange(mesAtual, mesAnterior),
      corretos,
      incorretos,
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }),
    });
  } catch (error) {
    console.error("Erro /api/registro-lacres-resumo:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/registro-lacres-graficos", requireAuth, async (req, res) => {
  try {
    const dados = await carregarLacresComCache();
    const filtrados = filtrarLacres(dados, req.query);

    function buildTop(field, limit = 10) {
      const map = lacreGroupCount(filtrados, field);
      const entries = Object.entries(map)
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit);

      return {
        labels: entries.map(([k]) => k),
        valores: entries.map(([, v]) => v),
      };
    }

    // por dia sempre de 01 a 31
    const dias = Array.from({ length: 31 }, (_, i) =>
      String(i + 1).padStart(2, "0")
    );
    const porDiaMap = Object.fromEntries(dias.map((d) => [d, 0]));

    filtrados.forEach((r) => {
      if (!r._dia) return;
      porDiaMap[r._dia] = (porDiaMap[r._dia] || 0) + 1;
    });

    let mesAtual = 0;
    let mesAnterior = 0;

    const validDates = filtrados
      .map((r) => r._dataObj)
      .filter(Boolean)
      .sort((a, b) => a - b);

    if (validDates.length) {
      const latest = validDates[validDates.length - 1];
      const currMonth = latest.getMonth();
      const currYear = latest.getFullYear();

      const prevRef = new Date(currYear, currMonth - 1, 1);
      const prevMonth = prevRef.getMonth();
      const prevYear = prevRef.getFullYear();

      // mês atual respeita os filtros completos
      mesAtual = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === currMonth &&
          r._dataObj.getFullYear() === currYear
      ).length;

      // mês anterior ignora apenas dataInicio/dataFim
      const querySemData = { ...req.query };
      delete querySemData.dataInicio;
      delete querySemData.dataFim;

      const baseComparativo = filtrarLacres(dados, querySemData);

      mesAnterior = baseComparativo.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === prevMonth &&
          r._dataObj.getFullYear() === prevYear
      ).length;
    }

    const corretoMap = {
      Correto: filtrados.filter(
        (r) => lacreNormLower(String(r["O LACRE ESTÁ CORRETO?"])) === "true"
      ).length,
      Incorreto: filtrados.filter((r) => {
        const v = lacreNormLower(String(r["O LACRE ESTÁ CORRETO?"]));
        return v && v !== "true";
      }).length,
      "Sem resposta": filtrados.filter(
        (r) => !lacreNorm(String(r["O LACRE ESTÁ CORRETO?"]))
      ).length,
    };

    const statusMap = lacreGroupCount(filtrados, "STATUS DE ENVIO");

    return res.json({
      porUnidade: buildTop("UNIDADE"),
      porTipoCarga: buildTop("TIPO DE CARGA"),
      porVigilante: buildTop("NOME DO VIGILANTE "),
      porDia: {
        labels: dias,
        valores: dias.map((d) => porDiaMap[d] || 0),
      },
      comparativoMensal: {
        mesAtual,
        mesAnterior,
      },
      lacreCorreto: {
        labels: Object.keys(corretoMap),
        valores: Object.values(corretoMap),
      },
      statusEnvio: {
        labels: Object.keys(statusMap),
        valores: Object.values(statusMap),
      },
    });
  } catch (error) {
    console.error("Erro /api/registro-lacres-graficos:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/registro-lacres-detalhes", requireAuth, async (req, res) => {
  try {
    const dados = await carregarLacresComCache();
    const filtrados = filtrarLacres(dados, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 20), 1), 200);

    const ordenados = [...filtrados].sort((a, b) => {
      const ad = a._dataObj ? a._dataObj.getTime() : 0;
      const bd = b._dataObj ? b._dataObj.getTime() : 0;
      return bd - ad;
    });

    const total = ordenados.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    const items = ordenados.slice(start, start + limit).map((r) => ({
      data: r._dataObj ? lacreFormatBR(r._dataObj) : r["DATA"] || "",
      unidade: r["UNIDADE"] || "",
      vigilante: r["NOME DO VIGILANTE "] || "",
      tipoCarga: r["TIPO DE CARGA"] || "",
      origem: r["ORIGEM"] || "",
      motorista: r["NOME DO MOTORISTA"] || "",
      placa: r["PLACA"] || "",
      lacreBau: r["N° LACRE BAÚ"] || "",
      lacreLateral: r["N° LACRE LATERAL"] || "",
      destino: r["DESTINO"] || "",
      statusEnvio: r["STATUS DE ENVIO"] || "",
      lacreCorreto: String(r["O LACRE ESTÁ CORRETO?"] || ""),
      confirmeNumero: r["CONFIRME O NÚMERO DO LACRE"] || "",
    }));

    return res.json({
      page: currentPage,
      limit,
      total,
      totalPages,
      items,
    });
  } catch (error) {
    console.error("Erro /api/registro-lacres-detalhes:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/registro-lacres-debug", requireAuth, async (req, res) => {
  try {
    const dados = await carregarLacresComCache();
    return res.json({
      total: dados.length,
      amostra: dados.slice(0, 5),
    });
  } catch (error) {
    console.error("Erro /api/registro-lacres-debug:", error);
    return res.status(500).json({ error: error.message });
  }
});

// ================== MATRIZ DE CONSEQUÊNCIA DASHBOARD ==================

app.get("/matriz-consequencia", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "matriz-consequencia.html"));
});

const MATRIZ_SPREADSHEET_ID = "15rRuS64cGBK1GOiuZwisnApORwQJoav4VB4I7Yq5ihA";
const MATRIZ_RANGE = "'Historico'!A1:M200000";

let matrizCache = null;
let matrizCacheTime = 0;
const MATRIZ_CACHE_TTL = 5 * 60 * 1000;

function mcNorm(value) {
  return String(value || "").trim();
}

function mcNormLower(value) {
  return mcNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function mcParseDate(value) {
  const str = mcNorm(value);
  if (!str) return null;

  let m = str.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) {
    const [, dd, mm, yyyy] = m;
    const d = new Date(`${yyyy}-${mm}-${dd}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) {
    const d = new Date(`${str}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const fallback = new Date(str);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function mcFormatISO(date) {
  if (!date) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function mcFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function mcSplitMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => mcNorm(v))
    .filter(Boolean);
}

function mcMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(mcNorm(fieldValue));
}

function mcUnique(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "pt-BR")
  );
}

function mcPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);
  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;
  return ((current - previous) / previous) * 100;
}

function mcGroupCount(rows, field) {
  const map = {};
  rows.forEach((row) => {
    const key = mcNorm(row[field]) || "NÃO INFORMADO";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

async function carregarMatrizRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: MATRIZ_SPREADSHEET_ID,
    range: MATRIZ_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const headers = values[0].map((h, i) => mcNorm(h) || `COL_${i + 1}`);
  const linhas = values.slice(1);

  return linhas
    .map((linha) => {
      const obj = {};
      headers.forEach((header, index) => {
        obj[header] = linha[index] ?? "";
      });

      obj._dataObj = mcParseDate(obj["Data da ocorrência"]);
      obj._conclusaoObj = mcParseDate(obj["Data Conclusão"]);
      obj._isoDate = obj._dataObj ? mcFormatISO(obj._dataObj) : "";
      obj._dia = obj._dataObj ? String(obj._dataObj.getDate()).padStart(2, "0") : "";
      obj._mesRef = obj._dataObj
        ? `${obj._dataObj.getFullYear()}-${String(obj._dataObj.getMonth() + 1).padStart(2, "0")}`
        : "";

      return obj;
    })
    .filter((row) =>
      mcNorm(row["Data da ocorrência"]) ||
      mcNorm(row["Colaborador"]) ||
      mcNorm(row["CPF"]) ||
      mcNorm(row["Filial"]) ||
      mcNorm(row["Violação"])
    );
}

async function carregarMatrizComCache() {
  const now = Date.now();

  if (matrizCache && now - matrizCacheTime < MATRIZ_CACHE_TTL) {
    return matrizCache;
  }

  matrizCache = await carregarMatrizRaw();
  matrizCacheTime = now;

  console.log("MATRIZ cache atualizado:", matrizCache.length);

  return matrizCache;
}

function filtrarMatriz(rows, query) {
  const dataInicio = mcNorm(query.dataInicio);
  const dataFim = mcNorm(query.dataFim);

  const filiais = mcSplitMulti(query.filial);
  const violacoes = mcSplitMulti(query.violacao);
  const descricoes = mcSplitMulti(query.descricao);
  const reincidencias = mcSplitMulti(query.reincidencia);
  const consequencias = mcSplitMulti(query.consequencia);
  const statusList = mcSplitMulti(query.status);
  const colaboradores = mcSplitMulti(query.colaborador);

  const busca = mcNormLower(query.busca);

  return rows.filter((row) => {
    if (dataInicio && (!row._isoDate || row._isoDate < dataInicio)) return false;
    if (dataFim && (!row._isoDate || row._isoDate > dataFim)) return false;

    if (!mcMatchesMulti(row["Filial"], filiais)) return false;
    if (!mcMatchesMulti(row["Violação"], violacoes)) return false;
    if (!mcMatchesMulti(row["Descrição da Violação"], descricoes)) return false;
    if (!mcMatchesMulti(row["Reincidencia"], reincidencias)) return false;
    if (!mcMatchesMulti(row["Consequencia"], consequencias)) return false;
    if (!mcMatchesMulti(row["Status"], statusList)) return false;
    if (!mcMatchesMulti(row["Colaborador"], colaboradores)) return false;

    if (busca) {
      const text = [
        row["Colaborador"],
        row["CPF"],
        row["Filial"],
        row["Violação"],
        row["Descrição da Violação"],
        row["Provas e comprovações"],
        row["Reincidencia"],
        row["Consequencia"],
        row["Status"],
      ].map(mcNormLower).join(" ");

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

app.get("/api/matriz-consequencia-filtros", requireAuth, async (req, res) => {
  try {
    const dados = await carregarMatrizComCache();

    return res.json({
      filiais: mcUnique(dados.map((r) => mcNorm(r["Filial"]))),
      violacoes: mcUnique(dados.map((r) => mcNorm(r["Violação"]))),
      descricoes: mcUnique(dados.map((r) => mcNorm(r["Descrição da Violação"]))),
      reincidencias: mcUnique(dados.map((r) => mcNorm(r["Reincidencia"]))),
      consequencias: mcUnique(dados.map((r) => mcNorm(r["Consequencia"]))),
      status: mcUnique(dados.map((r) => mcNorm(r["Status"]))),
      colaboradores: mcUnique(dados.map((r) => mcNorm(r["Colaborador"]))),
    });
  } catch (error) {
    console.error("Erro /api/matriz-consequencia-filtros:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/matriz-consequencia-resumo", requireAuth, async (req, res) => {
  try {
    const dados = await carregarMatrizComCache();
    const filtrados = filtrarMatriz(dados, req.query);

    const total = filtrados.length;

    const validDates = filtrados
      .map((r) => r._dataObj)
      .filter(Boolean)
      .sort((a, b) => a - b);

    let mesAtual = 0;
    let mesAnterior = 0;

    if (validDates.length) {
      const latest = validDates[validDates.length - 1];
      const currMonth = latest.getMonth();
      const currYear = latest.getFullYear();

      const prevRef = new Date(currYear, currMonth - 1, 1);
      const prevMonth = prevRef.getMonth();
      const prevYear = prevRef.getFullYear();

      mesAtual = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === currMonth &&
          r._dataObj.getFullYear() === currYear
      ).length;

      mesAnterior = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === prevMonth &&
          r._dataObj.getFullYear() === prevYear
      ).length;
    }

    const reincidentes = filtrados.filter((r) => {
      const v = mcNormLower(r["Reincidencia"]);
      return v === "sim" || v === "true";
    }).length;

    const concluidos = filtrados.filter((r) => {
      const v = mcNormLower(r["Status"]);
      return v.includes("conclu");
    }).length;

    return res.json({
      total,
      mesAtual,
      mesAnterior,
      variacaoMensal: mcPercentChange(mesAtual, mesAnterior),
      reincidentes,
      concluidos,
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }),
    });
  } catch (error) {
    console.error("Erro /api/matriz-consequencia-resumo:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/matriz-consequencia-graficos", requireAuth, async (req, res) => {
  try {
    const dados = await carregarMatrizComCache();
    const filtrados = filtrarMatriz(dados, req.query);

    function buildTop(field, limit = 10) {
      const map = mcGroupCount(filtrados, field);
      const entries = Object.entries(map)
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit);

      return {
        labels: entries.map(([k]) => k),
        valores: entries.map(([, v]) => v),
      };
    }

    const dias = Array.from({ length: 31 }, (_, i) =>
      String(i + 1).padStart(2, "0")
    );
    const porDiaMap = Object.fromEntries(dias.map((d) => [d, 0]));

    filtrados.forEach((r) => {
      if (!r._dia) return;
      porDiaMap[r._dia] = (porDiaMap[r._dia] || 0) + 1;
    });

    let mesAtual = 0;
    let mesAnterior = 0;

    const validDates = filtrados
      .map((r) => r._dataObj)
      .filter(Boolean)
      .sort((a, b) => a - b);

    if (validDates.length) {
      const latest = validDates[validDates.length - 1];
      const currMonth = latest.getMonth();
      const currYear = latest.getFullYear();

      const prevRef = new Date(currYear, currMonth - 1, 1);
      const prevMonth = prevRef.getMonth();
      const prevYear = prevRef.getFullYear();

      mesAtual = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === currMonth &&
          r._dataObj.getFullYear() === currYear
      ).length;

      mesAnterior = filtrados.filter(
        (r) =>
          r._dataObj &&
          r._dataObj.getMonth() === prevMonth &&
          r._dataObj.getFullYear() === prevYear
      ).length;
    }

    const reincidenciaMap = {
      Sim: filtrados.filter((r) => {
        const v = mcNormLower(r["Reincidencia"]);
        return v === "sim" || v === "true";
      }).length,
      Não: filtrados.filter((r) => {
        const v = mcNormLower(r["Reincidencia"]);
        return v === "nao" || v === "não" || v === "false";
      }).length,
      "Sem resposta": filtrados.filter((r) => !mcNorm(r["Reincidencia"])).length,
    };

    return res.json({
      porFilial: buildTop("Filial"),
      porViolacao: buildTop("Violação"),
      porConsequencia: buildTop("Consequencia"),
      porStatus: buildTop("Status"),
      porDia: {
        labels: dias,
        valores: dias.map((d) => porDiaMap[d] || 0),
      },
      comparativoMensal: {
        mesAtual,
        mesAnterior,
      },
      reincidencia: {
        labels: Object.keys(reincidenciaMap),
        valores: Object.values(reincidenciaMap),
      },
    });
  } catch (error) {
    console.error("Erro /api/matriz-consequencia-graficos:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/matriz-consequencia-detalhes", requireAuth, async (req, res) => {
  try {
    const dados = await carregarMatrizComCache();
    const filtrados = filtrarMatriz(dados, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 20), 1), 200);

    const ordenados = [...filtrados].sort((a, b) => {
      const ad = a._dataObj ? a._dataObj.getTime() : 0;
      const bd = b._dataObj ? b._dataObj.getTime() : 0;
      return bd - ad;
    });

    const total = ordenados.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    const items = ordenados.slice(start, start + limit).map((r) => ({
      dataOcorrencia: r._dataObj ? mcFormatBR(r._dataObj) : r["Data da ocorrência"] || "",
      colaborador: r["Colaborador"] || "",
      cpf: r["CPF"] || "",
      filial: r["Filial"] || "",
      violacao: r["Violação"] || "",
      descricao: r["Descrição da Violação"] || "",
      reincidencia: r["Reincidencia"] || "",
      consequencia: r["Consequencia"] || "",
      status: r["Status"] || "",
      dataConclusao: r._conclusaoObj ? mcFormatBR(r._conclusaoObj) : r["Data Conclusão"] || "",
    }));

    return res.json({
      page: currentPage,
      limit,
      total,
      totalPages,
      items,
    });
  } catch (error) {
    console.error("Erro /api/matriz-consequencia-detalhes:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/matriz-consequencia-debug", requireAuth, async (req, res) => {
  try {
    const dados = await carregarMatrizComCache();
    return res.json({
      total: dados.length,
      amostra: dados.slice(0, 5),
    });
  } catch (error) {
    console.error("Erro /api/matriz-consequencia-debug:", error);
    return res.status(500).json({ error: error.message });
  }
});

// ================== HC & ONBOARDING DASHBOARD ==================

app.get("/hc-onboarding", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "hc-onboarding.html"));
});

const HC_ONBOARDING_SPREADSHEET_ID = "1_1w_up0cxnqVZklpJV1cQRx6B5ptqQuGeWhWPpmNqfY";
const HC_ADMISSOES_RANGE = "'Admissões'!A1:Z200000";
const HC_ONBOARDING_RANGE = "'Onboarding'!A1:Z200000";

let hcOnboardingCache = null;
let hcOnboardingCacheTime = 0;
const HC_ONBOARDING_CACHE_TTL = 5 * 60 * 1000;

function hcoNorm(value) {
  return String(value || "").trim();
}

function hcoNormLower(value) {
  return hcoNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function hcoParseDate(value) {
  const str = hcoNorm(value);
  if (!str) return null;

  let m = str.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const [, dd, mm, yyyy, hh = "00", mi = "00", ss = "00"] = m;
    const d = new Date(`${yyyy}-${mm}-${dd}T${hh}:${mi}:${ss}`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) {
    const d = new Date(`${str}T00:00:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const fallback = new Date(str);
  return Number.isNaN(fallback.getTime()) ? null : fallback;
}

function hcoFormatISO(date) {
  if (!date) return "";
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function hcoFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function hcoSplitMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => hcoNorm(v))
    .filter(Boolean);
}

function hcoMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(hcoNorm(fieldValue));
}

function hcoUnique(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "pt-BR")
  );
}

function hcoPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);
  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;
  return ((current - previous) / previous) * 100;
}

function hcoGroupCount(rows, field) {
  const map = {};
  rows.forEach((row) => {
    const key = hcoNorm(row[field]) || "NÃO INFORMADO";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

function hcoFindHeader(headers, possibilities) {
  for (const p of possibilities) {
    const exact = headers.find((h) => hcoNormLower(h) === hcoNormLower(p));
    if (exact) return exact;
  }
  for (const p of possibilities) {
    const partial = headers.find((h) => hcoNormLower(h).includes(hcoNormLower(p)));
    if (partial) return partial;
  }
  return null;
}

async function carregarAdmissoesRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: HC_ONBOARDING_SPREADSHEET_ID,
    range: HC_ADMISSOES_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const headers = values[0].map((h, i) => hcoNorm(h) || `COL_${i + 1}`);

  const headerMap = {
    empresa: hcoFindHeader(headers, ["Empresa"]),
    colaborador: hcoFindHeader(headers, ["Colaborador(a)", "Colaborador", "Nome"]),
    data: hcoFindHeader(headers, ["Data Admissão", "Data Admissao"]),
    cargo: hcoFindHeader(headers, ["Cargo"]),
    unidade: hcoFindHeader(headers, ["Unidade"]),
  };

  return values.slice(1).map((line) => {
    const raw = {};
    headers.forEach((header, i) => {
      raw[header] = line[i] ?? "";
    });

    const data = headerMap.data ? raw[headerMap.data] || "" : "";
    const dataObj = hcoParseDate(data);

    return {
      origem: "Admissões",
      EMPRESA: headerMap.empresa ? raw[headerMap.empresa] || "" : "",
      COLABORADOR: headerMap.colaborador ? raw[headerMap.colaborador] || "" : "",
      DATA: data,
      CARGO: headerMap.cargo ? raw[headerMap.cargo] || "" : "",
      UNIDADE: headerMap.unidade ? raw[headerMap.unidade] || "" : "",
      _date: dataObj,
      _isoDate: dataObj ? hcoFormatISO(dataObj) : "",
      _day: dataObj ? String(dataObj.getDate()).padStart(2, "0") : "",
      _month: dataObj ? dataObj.getMonth() : null,
      _year: dataObj ? dataObj.getFullYear() : null,
    };
  }).filter((row) =>
    hcoNorm(row.COLABORADOR) ||
    hcoNorm(row.DATA) ||
    hcoNorm(row.UNIDADE) ||
    hcoNorm(row.EMPRESA)
  );
}

async function carregarOnboardingRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: HC_ONBOARDING_SPREADSHEET_ID,
    range: HC_ONBOARDING_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const headers = values[0].map((h, i) => hcoNorm(h) || `COL_${i + 1}`);

  const headerMap = {
    data: hcoFindHeader(headers, ["DATA", "Data"]),
    unidade: hcoFindHeader(headers, ["01. UNIDADE", "UNIDADE"]),
    nome: hcoFindHeader(headers, ["02. NOME COMPLETO", "NOME COMPLETO"]),
    cpf: hcoFindHeader(headers, ["03. CPF", "CPF"]),
    turno: hcoFindHeader(headers, ["04. TURNO", "TURNO"]),
    cargo: hcoFindHeader(headers, ["05. CARGO", "CARGO"]),
    empresa: hcoFindHeader(headers, ["06. EMPRESA", "EMPRESA"]),
    treinamento: hcoFindHeader(headers, ["07. TREINAMENTO", "TREINAMENTO"]),
    concorda: hcoFindHeader(headers, ["09. VOCÊ CONCORDA", "VOCÊ CONCORDA", "CONCORDA"]),
    instrutor: hcoFindHeader(headers, ["10. INSTRUTOR", "INSTRUTOR"]),
  };

  return values.slice(1).map((line) => {
    const raw = {};
    headers.forEach((header, i) => {
      raw[header] = line[i] ?? "";
    });

    const data = headerMap.data ? raw[headerMap.data] || "" : "";
    const dataObj = hcoParseDate(data);

    return {
      origem: "Onboarding",
      DATA: data,
      UNIDADE: headerMap.unidade ? raw[headerMap.unidade] || "" : "",
      COLABORADOR: headerMap.nome ? raw[headerMap.nome] || "" : "",
      CPF: headerMap.cpf ? raw[headerMap.cpf] || "" : "",
      TURNO: headerMap.turno ? raw[headerMap.turno] || "" : "",
      CARGO: headerMap.cargo ? raw[headerMap.cargo] || "" : "",
      EMPRESA: headerMap.empresa ? raw[headerMap.empresa] || "" : "",
      TREINAMENTO: headerMap.treinamento ? raw[headerMap.treinamento] || "" : "",
      CONCORDA: headerMap.concorda ? raw[headerMap.concorda] || "" : "",
      INSTRUTOR: headerMap.instrutor ? raw[headerMap.instrutor] || "" : "",
      _date: dataObj,
      _isoDate: dataObj ? hcoFormatISO(dataObj) : "",
      _day: dataObj ? String(dataObj.getDate()).padStart(2, "0") : "",
      _month: dataObj ? dataObj.getMonth() : null,
      _year: dataObj ? dataObj.getFullYear() : null,
    };
  }).filter((row) =>
    hcoNorm(row.COLABORADOR) ||
    hcoNorm(row.DATA) ||
    hcoNorm(row.UNIDADE)
  );
}

async function carregarHcOnboardingComCache() {
  const now = Date.now();

  if (hcOnboardingCache && now - hcOnboardingCacheTime < HC_ONBOARDING_CACHE_TTL) {
    return hcOnboardingCache;
  }

  const admissoes = await carregarAdmissoesRaw();
  const onboarding = await carregarOnboardingRaw();

  hcOnboardingCache = { admissoes, onboarding };
  hcOnboardingCacheTime = now;

  console.log("HC & ONBOARDING cache atualizado:", {
    admissoes: admissoes.length,
    onboarding: onboarding.length,
  });

  return hcOnboardingCache;
}

function filtrarAdmissoes(rows, query) {
  const dataInicio = hcoNorm(query.dataInicio);
  const dataFim = hcoNorm(query.dataFim);
  const empresas = hcoSplitMulti(query.admEmpresa);
  const unidades = hcoSplitMulti(query.admUnidade);
  const cargos = hcoSplitMulti(query.admCargo);
  const colaboradores = hcoSplitMulti(query.admColaborador);
  const busca = hcoNormLower(query.admBusca);

  return rows.filter((row) => {
    if (dataInicio && (!row._isoDate || row._isoDate < dataInicio)) return false;
    if (dataFim && (!row._isoDate || row._isoDate > dataFim)) return false;

    if (!hcoMatchesMulti(row.EMPRESA, empresas)) return false;
    if (!hcoMatchesMulti(row.UNIDADE, unidades)) return false;
    if (!hcoMatchesMulti(row.CARGO, cargos)) return false;
    if (!hcoMatchesMulti(row.COLABORADOR, colaboradores)) return false;

    if (busca) {
      const text = [
        row.EMPRESA,
        row.COLABORADOR,
        row.CARGO,
        row.UNIDADE,
      ].map(hcoNormLower).join(" ");

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function filtrarOnboarding(rows, query) {
  const dataInicio = hcoNorm(query.dataInicio);
  const dataFim = hcoNorm(query.dataFim);

  const unidades = hcoSplitMulti(query.onbUnidade);
  const cargos = hcoSplitMulti(query.onbCargo);
  const empresas = hcoSplitMulti(query.onbEmpresa);
  const treinamentos = hcoSplitMulti(query.onbTreinamento);
  const concordas = hcoSplitMulti(query.onbConcorda);
  const instrutores = hcoSplitMulti(query.onbInstrutor);
  const colaboradores = hcoSplitMulti(query.onbColaborador);
  const busca = hcoNormLower(query.onbBusca);

  return rows.filter((row) => {
    if (dataInicio && (!row._isoDate || row._isoDate < dataInicio)) return false;
    if (dataFim && (!row._isoDate || row._isoDate > dataFim)) return false;

    if (!hcoMatchesMulti(row.UNIDADE, unidades)) return false;
    if (!hcoMatchesMulti(row.CARGO, cargos)) return false;
    if (!hcoMatchesMulti(row.EMPRESA, empresas)) return false;
    if (!hcoMatchesMulti(row.TREINAMENTO, treinamentos)) return false;
    if (!hcoMatchesMulti(row.CONCORDA, concordas)) return false;
    if (!hcoMatchesMulti(row.INSTRUTOR, instrutores)) return false;
    if (!hcoMatchesMulti(row.COLABORADOR, colaboradores)) return false;

    if (busca) {
      const text = [
        row.UNIDADE,
        row.COLABORADOR,
        row.CPF,
        row.TURNO,
        row.CARGO,
        row.EMPRESA,
        row.TREINAMENTO,
        row.CONCORDA,
        row.INSTRUTOR,
      ].map(hcoNormLower).join(" ");

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

app.get("/api/hc-onboarding-filtros", requireAuth, async (req, res) => {
  try {
    const { admissoes, onboarding } = await carregarHcOnboardingComCache();

    return res.json({
      admissoes: {
        empresas: hcoUnique(admissoes.map((r) => hcoNorm(r.EMPRESA))),
        unidades: hcoUnique(admissoes.map((r) => hcoNorm(r.UNIDADE))),
        cargos: hcoUnique(admissoes.map((r) => hcoNorm(r.CARGO))),
        colaboradores: hcoUnique(admissoes.map((r) => hcoNorm(r.COLABORADOR))),
      },
      onboarding: {
        unidades: hcoUnique(onboarding.map((r) => hcoNorm(r.UNIDADE))),
        cargos: hcoUnique(onboarding.map((r) => hcoNorm(r.CARGO))),
        empresas: hcoUnique(onboarding.map((r) => hcoNorm(r.EMPRESA))),
        treinamentos: hcoUnique(onboarding.map((r) => hcoNorm(r.TREINAMENTO))),
        concordas: hcoUnique(onboarding.map((r) => hcoNorm(r.CONCORDA))),
        instrutores: hcoUnique(onboarding.map((r) => hcoNorm(r.INSTRUTOR))),
        colaboradores: hcoUnique(onboarding.map((r) => hcoNorm(r.COLABORADOR))),
      }
    });
  } catch (error) {
    console.error("Erro /api/hc-onboarding-filtros:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/hc-onboarding-resumo", requireAuth, async (req, res) => {
  try {
    const { admissoes, onboarding } = await carregarHcOnboardingComCache();
    const admFiltrados = filtrarAdmissoes(admissoes, req.query);
    const onbFiltrados = filtrarOnboarding(onboarding, req.query);

    function calcResumo(rows, extra = {}) {
      const total = rows.length;
      const validDates = rows.map((r) => r._date).filter(Boolean).sort((a, b) => a - b);

      let mesAtual = 0;
      let mesAnterior = 0;

      if (validDates.length) {
        const latest = validDates[validDates.length - 1];
        const currMonth = latest.getMonth();
        const currYear = latest.getFullYear();

        const prevRef = new Date(currYear, currMonth - 1, 1);
        const prevMonth = prevRef.getMonth();
        const prevYear = prevRef.getFullYear();

        mesAtual = rows.filter(
          (r) => r._date && r._date.getMonth() === currMonth && r._date.getFullYear() === currYear
        ).length;

        mesAnterior = rows.filter(
          (r) => r._date && r._date.getMonth() === prevMonth && r._date.getFullYear() === prevYear
        ).length;
      }

      return {
        total,
        mesAtual,
        mesAnterior,
        variacaoMensal: hcoPercentChange(mesAtual, mesAnterior),
        ...extra,
      };
    }

    const admEmpresas = new Set(admFiltrados.map((r) => hcoNorm(r.EMPRESA)).filter(Boolean)).size;
    const admUnidades = new Set(admFiltrados.map((r) => hcoNorm(r.UNIDADE)).filter(Boolean)).size;

    const onbInstrutores = new Set(onbFiltrados.map((r) => hcoNorm(r.INSTRUTOR)).filter(Boolean)).size;
    const onbConcordam = onbFiltrados.filter((r) => hcoNormLower(r.CONCORDA) === "sim").length;

    return res.json({
      admissoes: calcResumo(admFiltrados, {
        empresas: admEmpresas,
        unidades: admUnidades,
      }),
      onboarding: calcResumo(onbFiltrados, {
        instrutores: onbInstrutores,
        concordam: onbConcordam,
      }),
      ultimaAtualizacao: new Date().toLocaleTimeString("pt-BR", {
        timeZone: "America/Sao_Paulo",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }),
    });
  } catch (error) {
    console.error("Erro /api/hc-onboarding-resumo:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/hc-onboarding-graficos", requireAuth, async (req, res) => {
  try {
    const { admissoes, onboarding } = await carregarHcOnboardingComCache();
    const admFiltrados = filtrarAdmissoes(admissoes, req.query);
    const onbFiltrados = filtrarOnboarding(onboarding, req.query);

    function buildTop(rows, field, limit = 10) {
      const map = hcoGroupCount(rows, field);
      const entries = Object.entries(map)
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit);

      return {
        labels: entries.map(([k]) => k),
        valores: entries.map(([, v]) => v),
      };
    }

    const dias = Array.from({ length: 31 }, (_, i) => String(i + 1).padStart(2, "0"));

    const admPorDiaMap = Object.fromEntries(dias.map((d) => [d, 0]));
    admFiltrados.forEach((r) => {
      if (r._day) admPorDiaMap[r._day] = (admPorDiaMap[r._day] || 0) + 1;
    });

    const onbPorDiaMap = Object.fromEntries(dias.map((d) => [d, 0]));
    onbFiltrados.forEach((r) => {
      if (r._day) onbPorDiaMap[r._day] = (onbPorDiaMap[r._day] || 0) + 1;
    });

    const onbConcordaMap = {
      Sim: onbFiltrados.filter((r) => hcoNormLower(r.CONCORDA) === "sim").length,
      Não: onbFiltrados.filter((r) => {
        const v = hcoNormLower(r.CONCORDA);
        return v === "nao" || v === "não";
      }).length,
      "Sem resposta": onbFiltrados.filter((r) => !hcoNorm(r.CONCORDA)).length,
    };

    return res.json({
      admissoes: {
        porEmpresa: buildTop(admFiltrados, "EMPRESA"),
        porUnidade: buildTop(admFiltrados, "UNIDADE"),
        porCargo: buildTop(admFiltrados, "CARGO"),
        porDia: {
          labels: dias,
          valores: dias.map((d) => admPorDiaMap[d] || 0),
        },
      },
      onboarding: {
        porUnidade: buildTop(onbFiltrados, "UNIDADE"),
        porCargo: buildTop(onbFiltrados, "CARGO"),
        porInstrutor: buildTop(onbFiltrados, "INSTRUTOR"),
        porTreinamento: buildTop(onbFiltrados, "TREINAMENTO"),
        concordancia: {
          labels: Object.keys(onbConcordaMap),
          valores: Object.values(onbConcordaMap),
        },
        porDia: {
          labels: dias,
          valores: dias.map((d) => onbPorDiaMap[d] || 0),
        },
      }
    });
  } catch (error) {
    console.error("Erro /api/hc-onboarding-graficos:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/hc-onboarding-detalhes", requireAuth, async (req, res) => {
  try {
    const { admissoes, onboarding } = await carregarHcOnboardingComCache();
    const admFiltrados = filtrarAdmissoes(admissoes, req.query);
    const onbFiltrados = filtrarOnboarding(onboarding, req.query);

    const admPage = Math.max(Number(req.query.admPage || 1), 1);
    const onbPage = Math.max(Number(req.query.onbPage || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 15), 1), 100);

    const admOrdenados = [...admFiltrados].sort((a, b) => {
      const ad = a._date ? a._date.getTime() : 0;
      const bd = b._date ? b._date.getTime() : 0;
      return bd - ad;
    });

    const onbOrdenados = [...onbFiltrados].sort((a, b) => {
      const ad = a._date ? a._date.getTime() : 0;
      const bd = b._date ? b._date.getTime() : 0;
      return bd - ad;
    });

    const admTotalPages = Math.max(1, Math.ceil(admOrdenados.length / limit));
    const onbTotalPages = Math.max(1, Math.ceil(onbOrdenados.length / limit));

    const admCurrentPage = Math.min(admPage, admTotalPages);
    const onbCurrentPage = Math.min(onbPage, onbTotalPages);

    const admItems = admOrdenados.slice((admCurrentPage - 1) * limit, admCurrentPage * limit).map((r) => ({
      empresa: r.EMPRESA || "",
      colaborador: r.COLABORADOR || "",
      dataAdmissao: r._date ? hcoFormatBR(r._date) : r.DATA || "",
      cargo: r.CARGO || "",
      unidade: r.UNIDADE || "",
    }));

    const onbItems = onbOrdenados.slice((onbCurrentPage - 1) * limit, onbCurrentPage * limit).map((r) => ({
      data: r._date ? hcoFormatBR(r._date) : r.DATA || "",
      unidade: r.UNIDADE || "",
      colaborador: r.COLABORADOR || "",
      cpf: r.CPF || "",
      turno: r.TURNO || "",
      cargo: r.CARGO || "",
      empresa: r.EMPRESA || "",
      treinamento: r.TREINAMENTO || "",
      concorda: r.CONCORDA || "",
      instrutor: r.INSTRUTOR || "",
    }));

    return res.json({
      admissoes: {
        page: admCurrentPage,
        totalPages: admTotalPages,
        total: admOrdenados.length,
        items: admItems,
      },
      onboarding: {
        page: onbCurrentPage,
        totalPages: onbTotalPages,
        total: onbOrdenados.length,
        items: onbItems,
      },
    });
  } catch (error) {
    console.error("Erro /api/hc-onboarding-detalhes:", error);
    return res.status(500).json({ error: error.message });
  }
});

app.get("/api/hc-onboarding-debug", requireAuth, async (req, res) => {
  try {
    const data = await carregarHcOnboardingComCache();
    return res.json({
      admissoes: {
        total: data.admissoes.length,
        amostra: data.admissoes.slice(0, 3),
      },
      onboarding: {
        total: data.onboarding.length,
        amostra: data.onboarding.slice(0, 3),
      }
    });
  } catch (error) {
    console.error("Erro /api/hc-onboarding-debug:", error);
    return res.status(500).json({ error: error.message });
  }
});


// ================== SERVIDOR ==================

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Servidor rodando:");
  console.log(`http://localhost:${PORT}`);
});
