require("dotenv").config();

const express = require("express");
const session = require("express-session");
const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;
const path = require("path");
const { google } = require("googleapis");

const app = express();
const PORT = Number(process.env.PORT || 8080);

// ================== ENV ==================

const GOOGLE_CLIENT_ID =
  process.env.GOOGLE_CLIENT_ID ||
  process.env.ID_DO_CLIENTE_DO_GOOGLE ||
  "";

const GOOGLE_CLIENT_SECRET =
  process.env.GOOGLE_CLIENT_SECRET || "";

const GOOGLE_CALLBACK_URL =
  process.env.GOOGLE_CALLBACK_URL ||
  process.env.URL_DE_RETORNO_DE_CHAMADA_DO_GOOGLE ||
  "https://portal-security-fbs-production.up.railway.app/auth/google/callback";

const SESSION_SECRET =
  process.env.SESSION_SECRET ||
  "troque_essa_chave";

const CADASTRO_SHEET_ID =
  process.env.CADASTRO_SHEET_ID ||
  process.env.ID_DA_FOLHA_CADASTRO ||
  "";

const CADASTRO_USUARIOS_RANGE =
  process.env.CADASTRO_USUARIOS_RANGE ||
  "usuarios!A:R";

const CADASTRO_CARGOS_RANGE =
  process.env.CADASTRO_CARGOS_RANGE ||
  "cargos!A:Z";

const CADASTRO_NIVEIS_RANGE =
  process.env.CADASTRO_NIVEIS_RANGE ||
  "niveis_acesso!A:Z";

const googleOAuthEnabled =
  Boolean(GOOGLE_CLIENT_ID) && Boolean(GOOGLE_CLIENT_SECRET);

// ================== MIDDLEWARES ==================

app.set("trust proxy", 1);

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.use(
  session({
    name: "portal_security_sid",
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    proxy: true,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
      maxAge: 1000 * 60 * 60 * 12,
    },
  })
);

app.use(passport.initialize());

// Arquivos públicos como CSS, JS, imagens etc.
app.use(express.static(path.join(__dirname, "public")));

// ================== GOOGLE SHEETS ==================

async function conectarSheets() {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
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
    credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  const client = await auth.getClient();

  return google.sheets({
    version: "v4",
    auth: client,
  });
}
// ================== HELPERS ==================

function requireAuth(req, res, next) {
  if (!req.session?.userId) {
    return res.redirect("/login");
  }

  return next();
}

function requireAprovador(req, res, next) {
  if (!req.session?.userId) {
    return res.redirect("/login");
  }

  const perfil = String(req.session.perfil || "").toLowerCase();
  const aprovador = String(req.session.aprovador || "0");
  const nivel = String(req.session.nivel_acesso || "").toLowerCase();

  const podeAprovar =
    aprovador === "1" ||
    perfil === "aprovador" ||
    perfil === "master" ||
    perfil === "admin" ||
    nivel === "master" ||
    nivel === "admin" ||
    nivel === "aprovador";

  if (!podeAprovar) {
    return res.status(403).send("Acesso negado.");
  }

  return next();
}

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

function findHeaderIndex(headers, headerName) {
  return headers.findIndex(
    (h) => cadastroNormalizeLower(h) === cadastroNormalizeLower(headerName)
  );
}

async function getCadastroSheetObjects(range, writable = false) {
  if (!CADASTRO_SHEET_ID) {
    throw new Error("CADASTRO_SHEET_ID não configurado no Railway.");
  }

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
      (user) =>
        cadastroNormalizeLower(user.email) === cadastroNormalizeLower(email)
    ) || null
  );
}

async function getUsuariosSheetRaw() {
  if (!CADASTRO_SHEET_ID) {
    throw new Error("CADASTRO_SHEET_ID não configurado no Railway.");
  }

  const sheets = await conectarSheetsEdicao();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CADASTRO_SHEET_ID,
    range: CADASTRO_USUARIOS_RANGE,
  });

  const rows = response.data.values || [];
  const headers = rows[0] || [];

  return { sheets, rows, headers };
}

async function updateUsuarioGoogleInfoByEmail(email, googleProfile = {}) {
  const { sheets, rows, headers } = await getUsuariosSheetRaw();

  if (!rows.length) return false;

  const emailCol = findHeaderIndex(headers, "email");
  const nomeCol = findHeaderIndex(headers, "nome");
  const fotoCol = findHeaderIndex(headers, "foto");
  const googleIdCol = findHeaderIndex(headers, "google_id");
  const atualizadoCol = findHeaderIndex(headers, "atualizado_em");

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
  if (req.session?.userId) return res.redirect("/portal.html");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.get("/login", (req, res) => {
  if (req.session?.userId) return res.redirect("/portal.html");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

// ================== GOOGLE LOGIN / CADASTRO ==================

app.get("/auth/google", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.status(503).send("Login Google não configurado no servidor.");
  }

  return passport.authenticate("google", {
    scope: ["openid", "profile", "email"],
    session: false,
    prompt: "select_account",
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
    prompt: "select_account",
  })(req, res, next);
});

app.get("/auth/google/callback", (req, res, next) => {
  if (!googleOAuthEnabled) {
    req.session.loginError = "Login Google não configurado no servidor.";
    return res.redirect("/login");
  }

  return passport.authenticate("google", { session: false }, (err, user, info) => {
    if (err) {
      console.error("Erro no callback Google:", err);

      if (String(err.message || "").includes("CADASTRO_SHEET_ID")) {
        req.session.loginError =
          "CADASTRO_SHEET_ID não configurado no Railway.";
      } else {
        req.session.loginError = "Erro ao autenticar com Google.";
      }

      return req.session.save(() => res.redirect("/login"));
    }

    if (!user) {
      req.session.loginError = info?.message || "Acesso não autorizado.";
      return req.session.save(() => res.redirect("/login"));
    }

    if (user._registerFlow) {
      req.session.preCadastroUser = {
        nome: user.nome || "",
        email: user.email || "",
        foto: user.foto || "",
        google_id: user.google_id || "",
      };

      req.session.loginError = "";

      return req.session.save(() => {
        return res.redirect("/cadastro");
      });
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

    return req.session.save(() => {
      return res.redirect("/portal.html");
    });
  })(req, res, next);
});

// ================== CADASTRO ==================

app.get("/cadastro", (req, res) => {
  if (!req.session?.preCadastroUser) {
    return res.redirect("/auth/google/register");
  }

  return res.sendFile(path.join(__dirname, "public", "cadastro.html"));
});

app.get("/api/pre-cadastro-user", (req, res) => {
  const preUser = req.session?.preCadastroUser || null;

  if (!preUser) {
    return res
      .status(401)
      .json({ erro: "Usuário de pré-cadastro não autenticado." });
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
      novoId,
      dados.nome || "",
      dados.email || "",
      dados.login || "",
      "",
      dados.cargo || "",
      dados.area || "",
      "pendente",
      dados.nivel_acesso || "",
      dados.unidade || "",
      dados.google_id || "",
      "",
      "0",
      dados.foto || "",
      "1",
      now,
      dados.empresa || "",
      now,
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

    return req.session.save(() => {
      return res.json({ sucesso: true });
    });
  } catch (err) {
    console.error("Erro ao cadastrar usuário:", err);
    return res.status(500).json({ erro: "Erro ao cadastrar usuário" });
  }
});

// ================== APIs LOGIN ==================

app.get("/api/login-status", (req, res) => {
  const erro = req.session?.loginError || "";
  req.session.loginError = "";
  return req.session.save(() => {
    return res.json({ erro });
  });
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

// ================== APIs AUXILIARES ==================

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

// ================== ROTAS PROTEGIDAS ==================

app.get("/portal.html", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "portal.html"));
});

app.get("/portal", requireAuth, (req, res) => {
  return res.redirect("/portal.html");
});

app.get("/portal.html", requireAuth, (req, res) => {
  return res.redirect("/portal.html");
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

// ================== APROVAÇÕES ==================

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
        range: `usuarios!${columnToLetter(
          cadastroPendenteCol + 1
        )}${sheetRowNumber}`,
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
      return res
        .status(400)
        .json({ erro: "Nenhum campo válido para atualizar." });
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

// ================== LOGOUT ==================

app.get("/logout", (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error("Erro ao destruir sessão:", err);
      return res.redirect("/portal.html");
    }

    res.clearCookie("portal_security_sid");
    return res.redirect("/login");
  });
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
    const totalSecurity = filtrados.filter(
      (r) => dNormalizeLower(r[DESLIGADOS_COLUMNS.motivo]) === "security"
    ).length;
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
      totalSecurity,
      taxaBloqueio,
     ultimaAtualizacao: getHoraAtualizacaoBR(),
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
    const motivoOrdenado = Object.entries(motivoMap).sort((a, b) => b[1] - a[1]);

    const topMotivosSecurityMap = {};

    filtrados.forEach((row) => {
      const motivo = dNormalize(row[DESLIGADOS_COLUMNS.motivo]);
      const motivoLower = dNormalizeLower(motivo);

      if (motivoLower !== "security") return;

      const controle = dNormalize(row[DESLIGADOS_COLUMNS.controle]);
      const controleLower = dNormalizeLower(controle);

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
      motivoOrdenado,

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
    const dt = new Date(year, month - 1, day);
    return isNaN(dt) ? null : dt;
  }

  if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
    const [datePart] = str.split(" ");
    const [year, month, day] = datePart.split("-").map(Number);
    const dt = new Date(year, month - 1, day);
    return isNaN(dt) ? null : dt;
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

function ccoFormatBRDate(date) {
  if (!date) return "";
  const day = String(date.getDate()).padStart(2, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const year = date.getFullYear();
  return `${day}/${month}/${year}`;
}

function ccoIsUsefulHeader(header) {
  const h = ccoNormalize(header);
  return h && !/^unnamed/i.test(h);
}

function ccoLooksLikeDateHeader(header) {
  const h = ccoNormalizeLower(header);
  return /data|date|dia/.test(h);
}

function ccoIsSecurityHeader(header) {
  const h = ccoNormalizeLower(header);
  return /^security\s*\d+$/i.test(h) || /analista security/i.test(h);
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

function ccoParseMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => ccoNormalize(v))
    .filter(Boolean);
}

function ccoGroupCount(data, header) {
  const map = {};
  data.forEach((row) => {
    const key = ccoNormalize(row[header]) || "Sem valor";
    map[key] = (map[key] || 0) + 1;
  });
  return map;
}

function ccoGetMonthKey(date) {
  if (!date) return "";
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function ccoGetPreviousMonthKey(key) {
  if (!key) return "";
  const [yearStr, monthStr] = key.split("-");
  const year = Number(yearStr);
  const month = Number(monthStr);
  if (!year || !month) return "";

  const prev = month === 1
    ? new Date(year - 1, 11, 1)
    : new Date(year, month - 2, 1);

  return `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}`;
}

function ccoPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);

  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;

  return ((current - previous) / previous) * 100;
}

function ccoTopEntries(data, header, limit = 15, onlyFilled = true) {
  if (!header) return [];
  let base = data;

  if (onlyFilled) {
    base = data.filter((row) => ccoNormalize(row[header]));
  }

  return Object.entries(ccoGroupCount(base, header))
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);
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

async function ccoLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CCO_FBS_SPREADSHEET_ID,
    range: `'${CCO_FBS_SHEET_NAME}'!A1:AZ200000`,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) {
    return { headers: [], rows: [], sheetTitle: CCO_FBS_SHEET_NAME };
  }

  const headers = values[0].map((h, idx) => {
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

  return { headers, rows, sheetTitle: CCO_FBS_SHEET_NAME };
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

  const primaryDateColumn =
    ccoFindHeader(cleanHeaders, ["Data"]) ||
    ccoFindHeader(cleanHeaders, ["Data do ocorrido"]) ||
    dateColumns[0] ||
    null;

  const enrichedRows = cleanRows.map((row) => ({
    ...row,
    _primaryDate: primaryDateColumn ? ccoParseDate(row[primaryDateColumn]) : null,
  }));

  const tipoHeader = ccoFindHeader(cleanHeaders, [
    "Tipo de solicitação",
    "Tipo de solicitacao",
    "Tipo",
  ]);

  const ocorrenciaHeader = ccoFindHeader(cleanHeaders, [
    "Ocorrência",
    "Ocorrencia",
  ]);

  const unidadeHeader = ccoFindHeader(cleanHeaders, [
    "Unidade",
    "ID Unidade",
    "Site",
    "Base",
  ]);

  const setorHeader = ccoFindHeader(cleanHeaders, [
    "Setor",
  ]);

  const operacaoHeader = ccoFindHeader(cleanHeaders, [
    "Qual a operação?",
    "Qual a operacao?",
    "Operação",
    "Operacao",
  ]);

  const statusHeader = ccoFindHeader(cleanHeaders, [
    "Status da solicitação",
    "Status da solicitacao",
    "Status",
    "Situação",
    "Situacao",
  ]);

  const solicitanteHeader = ccoFindHeader(cleanHeaders, [
    "E-mail do solicitante",
    "Email do solicitante",
    "Nome do solicitante",
    "Solicitante",
    "Requisitante",
  ]);

  const slaHeader = ccoFindHeader(cleanHeaders, [
    "SLA",
  ]);

  const comoIdentificadoHeader = ccoFindHeader(cleanHeaders, [
    "Como identificado?",
    "Como identificado",
  ]);

  const filtroPreferencial = [
    tipoHeader,
    ocorrenciaHeader,
    setorHeader,
    operacaoHeader,
    unidadeHeader,
    statusHeader,
    slaHeader,
    comoIdentificadoHeader,
  ].filter(Boolean);

  ccoCache = {
    sheetTitle,
    headers: cleanHeaders,
    rows: enrichedRows,
    primaryDateColumn,
    dateColumns,
    filterHeaders: filtroPreferencial,
    tipoHeader,
    ocorrenciaHeader,
    unidadeHeader,
    setorHeader,
    operacaoHeader,
    statusHeader,
    solicitanteHeader,
    slaHeader,
    comoIdentificadoHeader,
  };

  ccoCacheTime = now;

  console.log("CCO cache atualizado:", {
    aba: sheetTitle,
    totalLinhas: enrichedRows.length,
    totalColunas: cleanHeaders.length,
    dataPrincipal: primaryDateColumn,
    tipoHeader,
    ocorrenciaHeader,
    unidadeHeader,
    setorHeader,
    operacaoHeader,
    statusHeader,
  });

  return ccoCache;
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
      tipoHeader: meta.tipoHeader,
      ocorrenciaHeader: meta.ocorrenciaHeader,
      unidadeHeader: meta.unidadeHeader,
      setorHeader: meta.setorHeader,
      operacaoHeader: meta.operacaoHeader,
      statusHeader: meta.statusHeader,
      filtrosDetectados: meta.filterHeaders,
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

    const filtros = meta.filterHeaders.map((header) => ({
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
    const filtrados = ccoApplyFilters(meta.rows, req.query, meta.filterHeaders);

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

    // =========================
    // COMPARATIVO MENSAL CORRIGIDO
    // =========================

    // Descobre o mês de referência:
    // 1) se o usuário filtrou um mês/ano por data inicial e final, usa esse mês
    // 2) caso contrário, usa o mês mais recente do conjunto filtrado
    let mesAtualKey = "";

    const dataInicio = ccoNormalize(req.query.dataInicio);
    const dataFim = ccoNormalize(req.query.dataFim);

    if (dataInicio) {
      const refDate = new Date(`${dataInicio}T00:00:00`);
      if (!isNaN(refDate)) {
        mesAtualKey = ccoGetMonthKey(refDate);
      }
    }

    if (!mesAtualKey && dataFim) {
      const refDate = new Date(`${dataFim}T00:00:00`);
      if (!isNaN(refDate)) {
        mesAtualKey = ccoGetMonthKey(refDate);
      }
    }

    if (!mesAtualKey) {
      const rowsWithDate = filtrados
        .filter((row) => row._primaryDate)
        .sort((a, b) => a._primaryDate - b._primaryDate);

      const latestDate = rowsWithDate.length
        ? rowsWithDate[rowsWithDate.length - 1]._primaryDate
        : null;

      mesAtualKey = latestDate ? ccoGetMonthKey(latestDate) : "";
    }

    const mesAnteriorKey = ccoGetPreviousMonthKey(mesAtualKey);

    // Remove apenas o recorte de data para conseguir comparar
    // mês atual x mês anterior mantendo os demais filtros
    const querySemData = {
      ...req.query,
      dataInicio: "",
      dataFim: "",
    };

    const baseComparativo = ccoApplyFilters(meta.rows, querySemData, meta.filterHeaders);

    const mesAtual = baseComparativo.filter(
      (row) => ccoGetMonthKey(row._primaryDate) === mesAtualKey
    ).length;

    const mesAnterior = baseComparativo.filter(
      (row) => ccoGetMonthKey(row._primaryDate) === mesAnteriorKey
    ).length;

    return res.json({
      total,
      totalUnidades,
      mesAtual,
      mesAnterior,
      variacaoMensal: ccoPercentChange(mesAtual, mesAnterior),
      statusDominante: topStatus ? topStatus[0] : "-",
      totalStatusDominante: topStatus ? topStatus[1] : 0,
      ultimaAtualizacao: getHoraAtualizacaoBR(),
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
    const filtrados = ccoApplyFilters(meta.rows, req.query, meta.filterHeaders);

    const tableAHeader = meta.tipoHeader || meta.headers[0] || "Tipo de solicitação";
    const tableBHeader = meta.ocorrenciaHeader || meta.headers[1] || "Ocorrência";

    const tableA = ccoTopEntries(filtrados, tableAHeader, 50, true).map(([nome, total]) => ({ nome, total }));
    const tableB = ccoTopEntries(filtrados, tableBHeader, 50, true).map(([nome, total]) => ({ nome, total }));

    const unidadeEntries = meta.unidadeHeader
      ? ccoTopEntries(filtrados, meta.unidadeHeader, 15, true)
      : [];

    const statusEntries = meta.statusHeader
      ? ccoTopEntries(filtrados, meta.statusHeader, 15, true)
      : [];

    const setorEntries = meta.setorHeader
      ? ccoTopEntries(filtrados, meta.setorHeader, 12, true)
      : [];

    const byDayMap = {};
    for (let i = 1; i <= 31; i++) {
      byDayMap[i] = 0;
    }

    filtrados.forEach((row) => {
      if (!row._primaryDate) return;
      const day = row._primaryDate.getDate();
      byDayMap[day] = (byDayMap[day] || 0) + 1;
    });

    return res.json({
      tableAHeader,
      tableBHeader,
      tableA,
      tableB,
      charts: [
        {
          header: meta.unidadeHeader || "Unidade",
          labels: unidadeEntries.map((x) => x[0]),
          values: unidadeEntries.map((x) => x[1]),
        },
        {
          header: meta.statusHeader || "Status",
          labels: statusEntries.map((x) => x[0]),
          values: statusEntries.map((x) => x[1]),
        },
        {
          header: meta.setorHeader || "Setor",
          labels: setorEntries.map((x) => x[0]),
          values: setorEntries.map((x) => x[1]),
        }
      ],
      porDia: {
        labels: Array.from({ length: 31 }, (_, i) => String(i + 1)),
        values: Array.from({ length: 31 }, (_, i) => byDayMap[i + 1] || 0),
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
    const filtrados = ccoApplyFilters(meta.rows, req.query, meta.filterHeaders);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 500);

    const sorted = [...filtrados].sort((a, b) => {
      const ad = a._primaryDate ? a._primaryDate.getTime() : 0;
      const bd = b._primaryDate ? b._primaryDate.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(Math.max(1, page), totalPages);
    const start = (currentPage - 1) * limit;

    const preferredHeaders = [
      meta.primaryDateColumn,
      meta.tipoHeader,
      meta.ocorrenciaHeader,
      meta.setorHeader,
      meta.operacaoHeader,
      meta.unidadeHeader,
      meta.statusHeader,
      meta.slaHeader,
      meta.comoIdentificadoHeader,
      "Data do ocorrido",
      "Local do ocorrido",
      "Responsável pelo caso",
      "Data de conclusão",
      "Devolutiva",
      "Motivo de recusa",
      "Relate com o máximo de detalhes o ocorrido:",
    ].filter(Boolean);

    const safeHeaders = [...new Set(
      preferredHeaders.filter((header) => meta.headers.includes(header))
    )];

    const rows = sorted.slice(start, start + limit).map((row) => {
      const obj = {};
      safeHeaders.forEach((header) => {
        obj[header] = row[header] || "";
      });
      return obj;
    });

    return res.json({
      headers: safeHeaders,
      total,
      page: currentPage,
      limit,
      totalPages,
      rows,
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.message,
    });
  }
});


// ================== GOOGLE SHEETS DASHBOARD RONDAS ==================

function normalizeText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeName(value) {
  if (!value) return "";

  return normalizeText(value)
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function normalizeStatus(value) {
  return normalizeText(value).toUpperCase();
}

function normalizeUnidade(value) {
  return normalizeText(value).toUpperCase();
}

const RONDAS_SPREADSHEET_ID = "1jFF45tBHXerhWWXC-X5fHgVyeUxKvx7Q0MiIwx8mUjU";

const ABAS_RONDAS = {
  geral: "Ronda geral",
  emergencia: "Ronda portas de emergência",
  estoque: "Ronda estoque RK"
};

async function buscarAbaRonda(abaKey) {
  const sheets = await conectarSheets();

  const abaReal = ABAS_RONDAS[abaKey];
  if (!abaReal) return [];

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: RONDAS_SPREADSHEET_ID,
    range: `'${abaReal}'!B:G`,
  });

  return response.data.values || [];
}

app.get("/api/rondas/:aba", requireAuth, async (req, res) => {
  try {
    const aba = String(req.params.aba || "").toLowerCase();

    if (!ABAS_RONDAS[aba]) {
      return res.status(400).json({ erro: "Aba inválida" });
    }

    const dados = await buscarAbaRonda(aba);
    const cabecalho = dados[0] || [];
    const linhas = dados.slice(1);

    const objetos = linhas.map((linha) => {
      const obj = {};

      cabecalho.forEach((col, i) => {
        const columnName = normalizeText(col);
        let value = linha[i] ?? "";

        if (
          columnName.toUpperCase().includes("NOME") ||
          columnName.toUpperCase().includes("COLABORADOR")
        ) {
          value = normalizeName(value);
        }

        if (columnName.toUpperCase().includes("STATUS")) {
          value = normalizeStatus(value);
        }

        if (columnName.toUpperCase().includes("UNIDADE")) {
          value = normalizeUnidade(value);
        }

        obj[columnName] = normalizeText(value);
      });

      return obj;
    });

    return res.json(objetos);
  } catch (e) {
    console.log("Erro /api/rondas/:aba:", e);
    return res.json([]);
  }
});

// ================== REGISTRO DE LACRES DASHBOARD ==================

app.get("/registro-lacres", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "registro-lacres.html"));
});

const LACRES_SPREADSHEET_ID = "1kxyx3nSpltdddSQkEGHHjOwcEtHMBd3RuQdk-w9q218";
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

function lacreGet(row, ...keys) {
  for (const key of keys) {
    if (
      row[key] !== undefined &&
      row[key] !== null &&
      String(row[key]).trim() !== ""
    ) {
      return row[key];
    }
  }
  return "";
}

function lacreVigilante(row) {
  return lacreGet(
    row,
    "NOME DO VIGILANTE",
    "NOME DO VIGILANTE ",
    "VIGILANTE",
    "NOME VIGILANTE"
  );
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
  return [...new Set(values.map(lacreNorm).filter(Boolean))].sort((a, b) =>
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

function lacreGroupCountCustom(rows, getter) {
  const map = {};

  rows.forEach((row) => {
    const key = lacreNorm(getter(row)) || "NÃO INFORMADO";
    map[key] = (map[key] || 0) + 1;
  });

  return map;
}

function buildTopFromMap(map, limit = 10) {
  const entries = Object.entries(map)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);

  return {
    labels: entries.map(([k]) => k),
    valores: entries.map(([, v]) => v),
  };
}

function buildTopCustom(rows, getter, limit = 10) {
  return buildTopFromMap(lacreGroupCountCustom(rows, getter), limit);
}

function getHoraAtualizacaoBR() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
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
      obj._dia = obj._dataObj
        ? String(obj._dataObj.getDate()).padStart(2, "0")
        : "";
      obj._mes = obj._dataObj
        ? `${obj._dataObj.getFullYear()}-${String(
            obj._dataObj.getMonth() + 1
          ).padStart(2, "0")}`
        : "";

      return obj;
    })
    .filter((row) => {
      return (
        lacreNorm(row["DATA"]) ||
        lacreNorm(row["UNIDADE"]) ||
        lacreNorm(lacreVigilante(row)) ||
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
  const lacreCorreto = lacreSplitMulti(query.lacreCorreto);

  const busca = lacreNormLower(query.busca);

  return rows.filter((row) => {
    if (dataInicio && (!row._isoDate || row._isoDate < dataInicio)) return false;
    if (dataFim && (!row._isoDate || row._isoDate > dataFim)) return false;

    if (!lacreMatchesMulti(row["UNIDADE"], unidades)) return false;
    if (!lacreMatchesMulti(row["TIPO DE CARGA"], tiposCarga)) return false;
    if (!lacreMatchesMulti(row["ORIGEM"], origens)) return false;
    if (!lacreMatchesMulti(row["DESTINO"], destinos)) return false;
    if (!lacreMatchesMulti(lacreVigilante(row), vigilantes)) return false;
    if (!lacreMatchesMulti(row["NOME DO MOTORISTA"], motoristas)) return false;
    if (!lacreMatchesMulti(row["PLACA"], placas)) return false;
    if (!lacreMatchesMulti(String(row["O LACRE ESTÁ CORRETO?"]), lacreCorreto)) return false;

    if (busca) {
      const text = [
        row["UNIDADE"],
        lacreVigilante(row),
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
      unidades: lacreUnique(dados.map((r) => r["UNIDADE"])),
      tiposCarga: lacreUnique(dados.map((r) => r["TIPO DE CARGA"])),
      origens: lacreUnique(dados.map((r) => r["ORIGEM"])),
      destinos: lacreUnique(dados.map((r) => r["DESTINO"])),
      vigilantes: lacreUnique(dados.map((r) => lacreVigilante(r))),
      motoristas: lacreUnique(dados.map((r) => r["NOME DO MOTORISTA"])),
      placas: lacreUnique(dados.map((r) => r["PLACA"])),
      lacreCorreto: lacreUnique(
        dados.map((r) => String(r["O LACRE ESTÁ CORRETO?"]))
      ),
      ultimaAtualizacao: getHoraAtualizacaoBR(),
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

    return res.json({
      total,
      mesAtual,
      mesAnterior,
      variacaoMensal: lacrePercentChange(mesAtual, mesAnterior),
      ultimaAtualizacao: getHoraAtualizacaoBR(),
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
      return buildTopFromMap(lacreGroupCount(filtrados, field), limit);
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

    return res.json({
      porUnidade: buildTop("UNIDADE"),
      porTipoCarga: buildTop("TIPO DE CARGA"),
      porVigilante: buildTopCustom(filtrados, lacreVigilante),
      porDia: {
        labels: dias,
        valores: dias.map((d) => porDiaMap[d] || 0),
      },
      comparativoMensal: {
        mesAtual,
        mesAnterior,
      },
      ultimaAtualizacao: getHoraAtualizacaoBR(),
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
      vigilante: lacreVigilante(r),
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
      headers: dados.length ? Object.keys(dados[0]).filter((h) => !h.startsWith("_")) : [],
      amostra: dados.slice(0, 5),
      vigilantesEncontrados: lacreUnique(dados.map((r) => lacreVigilante(r))).slice(0, 20),
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

// -------------------- helpers --------------------
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

function mcStartOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function mcEndOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function mcDiffDaysInclusive(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const s = mcStartOfDay(start);
  const e = mcStartOfDay(end);
  return Math.floor((e - s) / msPerDay) + 1;
}

function mcGetMonthKey(date) {
  if (!date) return "";
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function mcResolvePeriodFromQuery(query, rowsFiltrados) {
  // Compatibilidade com o front atual
  const dataInicio = mcNorm(query.dataInicio);
  const dataFim = mcNorm(query.dataFim);

  if (dataInicio || dataFim) {
    const start = dataInicio
      ? new Date(`${dataInicio}T00:00:00`)
      : (rowsFiltrados[0]?._dataObj ? mcStartOfDay(rowsFiltrados[0]._dataObj) : null);

    const end = dataFim
      ? new Date(`${dataFim}T23:59:59`)
      : (rowsFiltrados[rowsFiltrados.length - 1]?._dataObj ? mcEndOfDay(rowsFiltrados[rowsFiltrados.length - 1]._dataObj) : null);

    if (start && end && !Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime())) {
      return { start, end };
    }
  }

  // Novo modo: um único campo de período
  // Ex.: ?periodoTipo=dia&dataRef=2026-04-17
  // Ex.: ?periodoTipo=semana&dataRef=2026-04-17
  // Ex.: ?periodoTipo=mes&dataRef=2026-04-17
  // Ex.: ?periodoTipo=ano&dataRef=2026-04-17
  const periodoTipo = mcNormLower(query.periodoTipo);
  const dataRefStr = mcNorm(query.dataRef);

  if (periodoTipo && dataRefStr) {
    const ref = new Date(`${dataRefStr}T00:00:00`);
    if (!Number.isNaN(ref.getTime())) {
      if (periodoTipo === "dia") {
        return {
          start: mcStartOfDay(ref),
          end: mcEndOfDay(ref),
        };
      }

      if (periodoTipo === "semana") {
        const weekStart = new Date(ref);
        const day = weekStart.getDay(); // 0 dom, 1 seg...
        const diff = day === 0 ? -6 : 1 - day; // semana começando na segunda
        weekStart.setDate(weekStart.getDate() + diff);

        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        return {
          start: mcStartOfDay(weekStart),
          end: mcEndOfDay(weekEnd),
        };
      }

      if (periodoTipo === "mes") {
        const start = new Date(ref.getFullYear(), ref.getMonth(), 1);
        const end = new Date(ref.getFullYear(), ref.getMonth() + 1, 0, 23, 59, 59, 999);
        return { start, end };
      }

      if (periodoTipo === "ano") {
        const start = new Date(ref.getFullYear(), 0, 1);
        const end = new Date(ref.getFullYear(), 11, 31, 23, 59, 59, 999);
        return { start, end };
      }
    }
  }

  // fallback: usa o range real dos dados filtrados
  const validDates = rowsFiltrados
    .map((r) => r._dataObj)
    .filter(Boolean)
    .sort((a, b) => a - b);

  if (!validDates.length) return { start: null, end: null };

  return {
    start: mcStartOfDay(validDates[0]),
    end: mcEndOfDay(validDates[validDates.length - 1]),
  };
}

function mcGetComparisonWindow(startDate, endDate) {
  if (!startDate || !endDate) {
    return {
      currentStart: null,
      currentEnd: null,
      previousStart: null,
      previousEnd: null,
      label: "base anterior",
    };
  }

  const start = mcStartOfDay(startDate);
  const end = mcEndOfDay(endDate);

  const isSingleDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();

  const isFullMonth =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === 1 &&
    end.getDate() === new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();

  const isFullYear =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === 0 &&
    start.getDate() === 1 &&
    end.getMonth() === 11 &&
    end.getDate() === 31;

  const totalDays = mcDiffDaysInclusive(start, end);

  // dia anterior
  if (isSingleDay) {
    const prevStart = new Date(start);
    prevStart.setDate(prevStart.getDate() - 1);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart: mcStartOfDay(prevStart),
      previousEnd: mcEndOfDay(prevStart),
      label: "dia anterior",
    };
  }

  // mês anterior
  if (isFullMonth) {
    const previousStart = new Date(start.getFullYear(), start.getMonth() - 1, 1);
    const previousEnd = new Date(start.getFullYear(), start.getMonth(), 0, 23, 59, 59, 999);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart,
      previousEnd,
      label: "mês anterior",
    };
  }

  // ano anterior
  if (isFullYear) {
    const previousStart = new Date(start.getFullYear() - 1, 0, 1);
    const previousEnd = new Date(start.getFullYear() - 1, 11, 31, 23, 59, 59, 999);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart,
      previousEnd,
      label: "ano anterior",
    };
  }

  // qualquer outro período -> janela anterior do mesmo tamanho
  const previousEnd = new Date(start);
  previousEnd.setDate(previousEnd.getDate() - 1);
  previousEnd.setHours(23, 59, 59, 999);

  const previousStart = new Date(previousEnd);
  previousStart.setDate(previousStart.getDate() - (totalDays - 1));
  previousStart.setHours(0, 0, 0, 0);

  return {
    currentStart: start,
    currentEnd: end,
    previousStart,
    previousEnd,
    label: totalDays === 7 ? "semana anterior" : `período anterior (${totalDays} dias)`,
  };
}

// -------------------- carga --------------------
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

// -------------------- filtros --------------------
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

  // também entende periodoTipo + dataRef
  const fakePeriod = mcResolvePeriodFromQuery(query, rows);
  const periodStart = fakePeriod.start ? mcFormatISO(fakePeriod.start) : "";
  const periodEnd = fakePeriod.end ? mcFormatISO(fakePeriod.end) : "";

  return rows.filter((row) => {
    const startFilter = dataInicio || periodStart;
    const endFilter = dataFim || periodEnd;

    if (startFilter && (!row._isoDate || row._isoDate < startFilter)) return false;
    if (endFilter && (!row._isoDate || row._isoDate > endFilter)) return false;

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

// filtros sem considerar data, usados na comparação
function filtrarMatrizSemData(rows, query) {
  const cloneQuery = {
    ...query,
    dataInicio: "",
    dataFim: "",
    periodoTipo: "",
    dataRef: "",
  };

  return filtrarMatriz(rows, cloneQuery);
}

// -------------------- APIs --------------------
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

    const ordenadosPorData = filtrados
      .filter((r) => r._dataObj)
      .sort((a, b) => a._dataObj - b._dataObj);

    const periodo = mcResolvePeriodFromQuery(req.query, ordenadosPorData);
    const janela = mcGetComparisonWindow(periodo.start, periodo.end);

    const baseComparacao = filtrarMatrizSemData(dados, req.query);

    const casosPeriodoAtual = baseComparacao.filter((r) => {
      return r._dataObj && janela.currentStart && janela.currentEnd &&
        r._dataObj >= janela.currentStart &&
        r._dataObj <= janela.currentEnd;
    }).length;

    const casosPeriodoAnterior = baseComparacao.filter((r) => {
      return r._dataObj && janela.previousStart && janela.previousEnd &&
        r._dataObj >= janela.previousStart &&
        r._dataObj <= janela.previousEnd;
    }).length;

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
      mesAtual: casosPeriodoAtual,
      mesAnterior: casosPeriodoAnterior,
      variacaoMensal: mcPercentChange(casosPeriodoAtual, casosPeriodoAnterior),
      reincidentes,
      concluidos,
      comparativoLabel: janela.label || "base anterior",
      periodoAtualInicio: janela.currentStart ? mcFormatBR(janela.currentStart) : "",
      periodoAtualFim: janela.currentEnd ? mcFormatBR(janela.currentEnd) : "",
      periodoAnteriorInicio: janela.previousStart ? mcFormatBR(janela.previousStart) : "",
      periodoAnteriorFim: janela.previousEnd ? mcFormatBR(janela.previousEnd) : "",
      ultimaAtualizacao: getHoraAtualizacaoBR(),
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

    const ordenadosPorData = filtrados
      .filter((r) => r._dataObj)
      .sort((a, b) => a._dataObj - b._dataObj);

    const periodo = mcResolvePeriodFromQuery(req.query, ordenadosPorData);
    const janela = mcGetComparisonWindow(periodo.start, periodo.end);

    const baseComparacao = filtrarMatrizSemData(dados, req.query);

    const valorAtual = baseComparacao.filter((r) => {
      return r._dataObj && janela.currentStart && janela.currentEnd &&
        r._dataObj >= janela.currentStart &&
        r._dataObj <= janela.currentEnd;
    }).length;

    const valorAnterior = baseComparacao.filter((r) => {
      return r._dataObj && janela.previousStart && janela.previousEnd &&
        r._dataObj >= janela.previousStart &&
        r._dataObj <= janela.previousEnd;
    }).length;

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
        mesAtual: valorAtual,
        mesAnterior: valorAnterior,
        label: janela.label || "base anterior",
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

// ================== FIRST MILE ACCESS ==================

app.get("/first-mile-access", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "first-mile-access.html"));
});

const FM_SPREADSHEET_ID = "1kxePCOZeaVh48C7tP7Ua_iUpYg7EJttRvC7VAkEfQBo";
const FM_RANGE = "'Painel Operacional'!A:AO";

let fmCache = null;
let fmCacheTime = 0;
const FM_CACHE_TTL = 5 * 60 * 1000;

function fmNorm(v) {
  return String(v || "").trim();
}

function fmNormLower(v) {
  return fmNorm(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function fmHoraAtualizacao() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function fmParseDate(value) {
  const str = fmNorm(value);
  if (!str) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(str)) {
    const [d, m, y] = str.split("/").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  return null;
}

function fmFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function fmSplitMulti(value) {
  if (!value) return [];
  return String(value).split("|").map(v => fmNorm(v)).filter(Boolean);
}

function fmMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(fmNorm(fieldValue));
}

function fmResolveHeader(headers, aliases) {
  const normalized = headers.map(h => ({
    original: h,
    norm: fmNormLower(h),
  }));

  for (const alias of aliases) {
    const aliasNorm = fmNormLower(alias);
    const exact = normalized.find(h => h.norm === aliasNorm);
    if (exact) return exact.original;
  }

  for (const alias of aliases) {
    const aliasNorm = fmNormLower(alias);
    const partial = normalized.find(h => h.norm.includes(aliasNorm));
    if (partial) return partial.original;
  }

  return null;
}

function fmGetValue(row, key) {
  return fmNorm(row?.[key] || "");
}

function fmCountDistinct(rows, key) {
  return new Set(
    rows.map(r => fmGetValue(r, key)).filter(Boolean)
  ).size;
}

function fmGroupCount(rows, key) {
  const map = {};
  rows.forEach(row => {
    const val = fmGetValue(row, key) || "Sem informação";
    map[val] = (map[val] || 0) + 1;
  });

  return Object.entries(map)
    .map(([nome, total]) => ({ nome, total }))
    .sort((a, b) => b.total - a.total);
}

async function fmLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: FM_SPREADSHEET_ID,
    range: FM_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) {
    return { headers: [], rows: [], map: {} };
  }

  const headers = values[0].map((h, i) => fmNorm(h) || `COL_${i + 1}`);
  const dataRows = values.slice(1);

  const rows = dataRows.map(line => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = line[i] ?? "";
    });
    return obj;
  });

  const map = {
    data: fmResolveHeader(headers, ["Date", "Data", "Created At"]),
    agency: fmResolveHeader(headers, ["Agency", "Hub", "Station", "Origin Agency"]),
    regional: fmResolveHeader(headers, ["Regional", "Region"]),
    city: fmResolveHeader(headers, ["City", "Cidade"]),
    vehicle: fmResolveHeader(headers, ["Vehicle", "Vehicle Type", "Tipo de Veículo"]),
    monitor: fmResolveHeader(headers, ["Monitor", "Monitors", "Monitoring"]),
    shopName: fmResolveHeader(headers, ["Shop Name", "Seller", "Seller Name"]),
    driver: fmResolveHeader(headers, ["Driver", "Driver Name", "Motorista"]),
    statusFinal: fmResolveHeader(headers, ["Final Status", "Status Final"]),
    ocorrenciaValidacao: fmResolveHeader(headers, ["Validation of Occurrence", "Occurrence Validation", "Validação da Ocorrência"]),
    rota: fmResolveHeader(headers, ["Route ID", "Route", "Rota", "Route Code"]),
  };

  const enriched = rows.map(row => {
    const dt = fmParseDate(fmGetValue(row, map.data));
    return {
      ...row,
      _dateObj: dt,
      _day: dt ? dt.getDate() : null,
      _month: dt ? dt.getMonth() + 1 : null,
      _year: dt ? dt.getFullYear() : null,
    };
  });

  return { headers, rows: enriched, map };
}

async function fmLoadWithCache() {
  const now = Date.now();

  if (fmCache && now - fmCacheTime < FM_CACHE_TTL) {
    return fmCache;
  }

  fmCache = await fmLoadRaw();
  fmCacheTime = now;

  console.log("FIRST MILE ACCESS cache atualizado:", fmCache.rows.length);

  return fmCache;
}

function fmApplyFilters(rows, query, map) {
  const agency = fmSplitMulti(query.agency);
  const regional = fmSplitMulti(query.regional);
  const city = fmSplitMulti(query.city);
  const vehicle = fmSplitMulti(query.vehicle);
  const monitor = fmSplitMulti(query.monitor);
  const busca = fmNormLower(query.busca);

  const dataInicio = query.dataInicio ? new Date(`${query.dataInicio}T00:00:00`) : null;
  const dataFim = query.dataFim ? new Date(`${query.dataFim}T23:59:59`) : null;

  return rows.filter((row) => {
    if (dataInicio && (!row._dateObj || row._dateObj < dataInicio)) return false;
    if (dataFim && (!row._dateObj || row._dateObj > dataFim)) return false;

    if (map.agency && !fmMatchesMulti(fmGetValue(row, map.agency), agency)) return false;
    if (map.regional && !fmMatchesMulti(fmGetValue(row, map.regional), regional)) return false;
    if (map.city && !fmMatchesMulti(fmGetValue(row, map.city), city)) return false;
    if (map.vehicle && !fmMatchesMulti(fmGetValue(row, map.vehicle), vehicle)) return false;
    if (map.monitor && !fmMatchesMulti(fmGetValue(row, map.monitor), monitor)) return false;

    if (busca) {
      const text = Object.values(row)
        .filter(v => typeof v !== "object")
        .join(" ")
        .toLowerCase();

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function fmMonthLabel(month) {
  const labels = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
  };
  return labels[month] || String(month);
}

// FILTROS
app.get("/api/fm-access-filtros", requireAuth, async (req, res) => {
  try {
    const { rows, map } = await fmLoadWithCache();

    const uniq = (key) => {
      if (!key) return [];
      return [...new Set(rows.map(r => fmGetValue(r, key)).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));
    };

    return res.json({
      agencies: uniq(map.agency),
      regionals: uniq(map.regional),
      cities: uniq(map.city),
      vehicles: uniq(map.vehicle),
      monitors: uniq(map.monitor),
      ultimaAtualizacao: fmHoraAtualizacao(),
    });
  } catch (error) {
    console.error("Erro /api/fm-access-filtros:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// GRAFICOS E KPIS
app.get("/api/fm-access-graficos", requireAuth, async (req, res) => {
  try {
    const { rows, map } = await fmLoadWithCache();
    const filtered = fmApplyFilters(rows, req.query, map);

    const totalRotas = map.rota
      ? fmCountDistinct(filtered, map.rota)
      : filtered.length;

    const totalSellers = map.shopName
      ? fmCountDistinct(filtered, map.shopName)
      : 0;

    const totalCities = map.city
      ? fmCountDistinct(filtered, map.city)
      : 0;

    const totalDrivers = map.driver
      ? fmCountDistinct(filtered, map.driver)
      : 0;

    const totalOcorrencias = filtered.length;

    const porAgency = map.agency ? fmGroupCount(filtered, map.agency).slice(0, 12) : [];
    const porRegional = map.regional ? fmGroupCount(filtered, map.regional).slice(0, 10) : [];
    const porStatusFinal = map.statusFinal ? fmGroupCount(filtered, map.statusFinal).slice(0, 10) : [];
    const porMonitor = map.monitor ? fmGroupCount(filtered, map.monitor).slice(0, 10) : [];
    const porCity = map.city ? fmGroupCount(filtered, map.city).slice(0, 10) : [];
    const distribuicao = map.ocorrenciaValidacao ? fmGroupCount(filtered, map.ocorrenciaValidacao).slice(0, 12) : [];

    const diaMap = {};
    for (let i = 1; i <= 31; i++) diaMap[i] = 0;
    filtered.forEach(r => {
      if (r._day) diaMap[r._day] += 1;
    });

    const mesMap = {};
    filtered.forEach(r => {
      if (r._month) mesMap[r._month] = (mesMap[r._month] || 0) + 1;
    });

    return res.json({
      resumo: {
        totalRotas,
        totalSellers,
        totalCities,
        totalDrivers,
        totalOcorrencias,
      },
      porAgency,
      porRegional,
      porStatusFinal,
      porMonitor,
      porCity,
      distribuicao,
      porDia: {
        labels: Array.from({ length: 31 }, (_, i) => String(i + 1)),
        values: Array.from({ length: 31 }, (_, i) => diaMap[i + 1] || 0),
      },
      porMes: {
        labels: Object.keys(mesMap)
          .map(Number)
          .sort((a, b) => a - b)
          .map(fmMonthLabel),
        values: Object.keys(mesMap)
          .map(Number)
          .sort((a, b) => a - b)
          .map(m => mesMap[m] || 0),
      },
      ultimaAtualizacao: fmHoraAtualizacao(),
    });
  } catch (error) {
    console.error("Erro /api/fm-access-graficos:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// DETALHES
app.get("/api/fm-access-detalhes", requireAuth, async (req, res) => {
  try {
    const { headers, rows, map } = await fmLoadWithCache();
    const filtered = fmApplyFilters(rows, req.query, map);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);

    const sorted = [...filtered].sort((a, b) => {
      const ad = a._dateObj ? a._dateObj.getTime() : 0;
      const bd = b._dateObj ? b._dateObj.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;
    const pageRows = sorted.slice(start, start + limit);

    const cleanHeaders = headers.filter(h => !String(h).startsWith("_"));

    return res.json({
      headers: cleanHeaders,
      rows: pageRows.map(row => {
        const obj = {};
        cleanHeaders.forEach(h => {
          obj[h] = row[h] ?? "";
        });
        return obj;
      }),
      total,
      page: currentPage,
      totalPages,
      ultimaAtualizacao: fmHoraAtualizacao(),
    });
  } catch (error) {
    console.error("Erro /api/fm-access-detalhes:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== CHECKLIST DASHBOARD ==================

app.get("/checklist", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "checklist.html"));
});

const CHECKLIST_SPREADSHEET_ID = "1vUv0AJ_bASBkkzxUWOyheJSASb4VDsaRKAl40EzRKsk";
const CHECKLIST_RANGE = "'Respostas'!A1:Z200000";

let checklistCache = null;
let checklistCacheTime = 0;
const CHECKLIST_CACHE_TTL = 5 * 60 * 1000;

// ================== HELPERS ==================

function clNorm(value) {
  return String(value || "").trim();
}

function clNormLower(value) {
  return clNorm(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function clParseDate(value) {
  const str = clNorm(value);
  if (!str) return null;

  if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
    const [datePart] = str.split(" ");
    const [year, month, day] = datePart.split("-").map(Number);
    const dt = new Date(year, month - 1, day);
    return isNaN(dt.getTime()) ? null : dt;
  }

  if (/^\d{1,2}\/\d{1,2}\/\d{4}/.test(str)) {
    const [datePart] = str.split(" ");
    const parts = datePart.split("/").map(Number);
    if (parts.length === 3) {
      const [a, b, c] = parts;

      let dt = new Date(c, a - 1, b);
      if (!isNaN(dt.getTime())) return dt;

      dt = new Date(c, b - 1, a);
      if (!isNaN(dt.getTime())) return dt;
    }
  }

  return null;
}

function clFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function clSplitMulti(value) {
  if (!value) return [];
  return String(value)
    .split("|")
    .map((v) => clNorm(v))
    .filter(Boolean);
}

function clMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(clNorm(fieldValue));
}

function clPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);

  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;

  return ((current - previous) / previous) * 100;
}

function clStartOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function clEndOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function clDiffDaysInclusive(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const s = clStartOfDay(start);
  const e = clStartOfDay(end);
  return Math.floor((e - s) / msPerDay) + 1;
}

function clGetHoraAtualizacaoBR() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function clResolvePeriodFromQuery(query, groups) {
  const periodoTipo = clNormLower(query.periodoTipo || "mes");
  const dataRef = clNorm(query.dataRef);

  if (dataRef) {
    const ref = new Date(`${dataRef}T00:00:00`);

    if (!isNaN(ref.getTime())) {
      if (periodoTipo === "dia") {
        return {
          start: clStartOfDay(ref),
          end: clEndOfDay(ref),
        };
      }

      if (periodoTipo === "semana") {
        const weekStart = new Date(ref);
        const day = weekStart.getDay();
        const diff = day === 0 ? -6 : 1 - day;
        weekStart.setDate(weekStart.getDate() + diff);

        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        return {
          start: clStartOfDay(weekStart),
          end: clEndOfDay(weekEnd),
        };
      }

      if (periodoTipo === "ano") {
        return {
          start: new Date(ref.getFullYear(), 0, 1),
          end: new Date(ref.getFullYear(), 11, 31, 23, 59, 59, 999),
        };
      }

      return {
        start: new Date(ref.getFullYear(), ref.getMonth(), 1),
        end: new Date(ref.getFullYear(), ref.getMonth() + 1, 0, 23, 59, 59, 999),
      };
    }
  }

  const validDates = groups
    .map((g) => g._dateObj)
    .filter(Boolean)
    .sort((a, b) => a - b);

  if (!validDates.length) {
    return { start: null, end: null };
  }

  const latest = validDates[validDates.length - 1];

  return {
    start: new Date(latest.getFullYear(), latest.getMonth(), 1),
    end: new Date(latest.getFullYear(), latest.getMonth() + 1, 0, 23, 59, 59, 999),
  };
}

function clGetComparisonWindow(startDate, endDate) {
  if (!startDate || !endDate) {
    return {
      currentStart: null,
      currentEnd: null,
      previousStart: null,
      previousEnd: null,
      label: "base anterior",
    };
  }

  const start = clStartOfDay(startDate);
  const end = clEndOfDay(endDate);

  const isSingleDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();

  const isFullMonth =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === 1 &&
    end.getDate() === new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();

  const isFullYear =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === 0 &&
    start.getDate() === 1 &&
    end.getMonth() === 11 &&
    end.getDate() === 31;

  const totalDays = clDiffDaysInclusive(start, end);

  if (isSingleDay) {
    const prev = new Date(start);
    prev.setDate(prev.getDate() - 1);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart: clStartOfDay(prev),
      previousEnd: clEndOfDay(prev),
      label: "dia anterior",
    };
  }

  if (isFullMonth) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear(), start.getMonth() - 1, 1),
      previousEnd: new Date(start.getFullYear(), start.getMonth(), 0, 23, 59, 59, 999),
      label: "mês anterior",
    };
  }

  if (isFullYear) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear() - 1, 0, 1),
      previousEnd: new Date(start.getFullYear() - 1, 11, 31, 23, 59, 59, 999),
      label: "ano anterior",
    };
  }

  const previousEnd = new Date(start);
  previousEnd.setDate(previousEnd.getDate() - 1);
  previousEnd.setHours(23, 59, 59, 999);

  const previousStart = new Date(previousEnd);
  previousStart.setDate(previousStart.getDate() - (totalDays - 1));
  previousStart.setHours(0, 0, 0, 0);

  return {
    currentStart: start,
    currentEnd: end,
    previousStart,
    previousEnd,
    label: totalDays === 7 ? "semana anterior" : `período anterior (${totalDays} dias)`,
  };
}

// ================== LOAD RAW ==================

async function clLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: CHECKLIST_SPREADSHEET_ID,
    range: CHECKLIST_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) {
    return [];
  }

  const headers = values[0].map((h, i) => clNorm(h) || `COL_${i + 1}`);
  const rows = values.slice(1);

  return rows.map((line) => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = line[i] ?? "";
    });

    obj._dateObj = clParseDate(obj["data_checklist"]);
    obj._day = obj._dateObj ? obj._dateObj.getDate() : null;
    obj._monthKey = obj._dateObj
      ? `${obj._dateObj.getFullYear()}-${String(obj._dateObj.getMonth() + 1).padStart(2, "0")}`
      : "";

    return obj;
  });
}

// ================== CACHE ==================

async function clLoadWithCache() {
  const now = Date.now();

  if (checklistCache && now - checklistCacheTime < CHECKLIST_CACHE_TTL) {
    return checklistCache;
  }

  const rows = await clLoadRaw();

  checklistCache = rows;
  checklistCacheTime = now;

  console.log("CHECKLIST cache atualizado:", rows.length);

  return checklistCache;
}

// ================== GROUP BY registro_id ==================

function clGroupByChecklist(rows) {
  const map = new Map();

  rows.forEach((row) => {
    const id = clNorm(row["registro_id"]);
    if (!id) return;

    if (!map.has(id)) {
      map.set(id, {
        registro_id: id,
        data_checklist: row["data_checklist"] || "",
        elaborado_por: row["elaborado_por"] || "",
        funcao: row["funcao"] || "",
        unidade: row["unidade"] || "",
        estado: row["estado"] || "",
        cidade: row["cidade"] || "",
        _dateObj: row._dateObj || null,
        itens: [],
      });
    }

    map.get(id).itens.push(row);
  });

  return [...map.values()].map((group) => {
    const totalItens = group.itens.length;

    const itensNc = group.itens.filter((item) => {
      const geraNc = clNormLower(item["gera_nc"]);
      return geraNc === "sim" || geraNc === "s" || geraNc === "yes" || geraNc === "true";
    }).length;

    const pendencias = group.itens.filter((item) => {
      const prazo = clNormLower(item["situacao_prazo"]);
      return prazo.includes("pend") || prazo.includes("aberto") || prazo.includes("atras");
    }).length;

    const pontuacoes = group.itens
      .map((item) => Number(String(item["pontuacao"] || "").replace(",", ".")))
      .filter((n) => !isNaN(n));

    const mediaPontuacao = pontuacoes.length
      ? pontuacoes.reduce((sum, n) => sum + n, 0) / pontuacoes.length
      : 0;

    return {
      ...group,
      totalItens,
      totalNc: itensNc,
      pendencias,
      mediaPontuacao,
      conforme: itensNc === 0,
    };
  });
}

// ================== FILTERS ==================

function clApplyFilters(rows, query) {
  const unidade = clSplitMulti(query.unidade);
  const elaboradoPor = clSplitMulti(query.elaborador || query.elaboradoPor);
  const funcao = clSplitMulti(query.funcao);
  const estado = clSplitMulti(query.estado);
  const cidade = clSplitMulti(query.cidade);
  const topico = clSplitMulti(query.topico);
  const geraNc = clSplitMulti(query.geraNc);
  const prazo = clSplitMulti(query.prazo || query.situacaoPrazo);
  const area = clSplitMulti(query.area || query.areaResponsavel);
  const busca = clNormLower(query.busca);

  return rows.filter((row) => {
    if (!clMatchesMulti(row["unidade"], unidade)) return false;
    if (!clMatchesMulti(row["elaborado_por"], elaboradoPor)) return false;
    if (!clMatchesMulti(row["funcao"], funcao)) return false;
    if (!clMatchesMulti(row["estado"], estado)) return false;
    if (!clMatchesMulti(row["cidade"], cidade)) return false;
    if (!clMatchesMulti(row["topico"], topico)) return false;
    if (!clMatchesMulti(row["gera_nc"], geraNc)) return false;
    if (!clMatchesMulti(row["situacao_prazo"], prazo)) return false;
    if (!clMatchesMulti(row["area_responsavel"], area)) return false;

    if (busca) {
      const text = Object.values(row)
        .filter((v) => typeof v !== "object")
        .join(" ")
        .toLowerCase();

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function clApplyFiltersWithoutPeriod(rows, query) {
  return clApplyFilters(rows, {
    ...query,
    periodoTipo: "",
    dataRef: "",
  });
}

function clFilterGroupsByPeriod(groups, query) {
  const period = clResolvePeriodFromQuery(query, groups);

  if (!period.start || !period.end) {
    return { groups, period };
  }

  const filtered = groups.filter((group) => {
    const dt = group._dateObj;
    return dt && dt >= period.start && dt <= period.end;
  });

  return { groups: filtered, period };
}

// ================== FILTROS ==================

app.get("/api/checklist-filtros", requireAuth, async (req, res) => {
  try {
    const rows = await clLoadWithCache();

    const uniq = (field) =>
      [...new Set(rows.map((r) => clNorm(r[field])).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json({
      unidade: uniq("unidade"),
      elaboradoPor: uniq("elaborado_por"),
      elaborador: uniq("elaborado_por"),
      funcao: uniq("funcao"),
      estado: uniq("estado"),
      cidade: uniq("cidade"),
      topico: uniq("topico"),
      geraNc: uniq("gera_nc"),
      situacaoPrazo: uniq("situacao_prazo"),
      prazo: uniq("situacao_prazo"),
      areaResponsavel: uniq("area_responsavel"),
      area: uniq("area_responsavel"),
      ultimaAtualizacao: clGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/checklist-filtros:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== RESUMO ==================

app.get("/api/checklist-resumo", requireAuth, async (req, res) => {
  try {
    const rows = await clLoadWithCache();

    const filteredRows = clApplyFilters(rows, req.query);
    const groups = clGroupByChecklist(filteredRows);

    const { groups: periodGroups, period } = clFilterGroupsByPeriod(groups, req.query);

    const total = periodGroups.length;
    const ok = periodGroups.filter((g) => g.conforme).length;
    const nok = total - ok;
    const taxa = total ? (ok / total) * 100 : 0;
    const pendencias = periodGroups.reduce((sum, g) => sum + g.pendencias, 0);
    const media = total
      ? periodGroups.reduce((sum, g) => sum + g.mediaPontuacao, 0) / total
      : 0;

    const comparison = clGetComparisonWindow(period.start, period.end);

    const baseNoPeriodRows = clApplyFiltersWithoutPeriod(rows, req.query);
    const baseNoPeriodGroups = clGroupByChecklist(baseNoPeriodRows);

    const anterior = baseNoPeriodGroups.filter((g) => {
      const dt = g._dateObj;
      return (
        dt &&
        comparison.previousStart &&
        comparison.previousEnd &&
        dt >= comparison.previousStart &&
        dt <= comparison.previousEnd
      );
    }).length;

    return res.json({
      total,
      ok,
      nok,
      taxa,
      pendencias,
      media,
      anterior,
      comparativoLabel: comparison.label || "base anterior",
      periodoAtualInicio: comparison.currentStart ? clFormatBR(comparison.currentStart) : "",
      periodoAtualFim: comparison.currentEnd ? clFormatBR(comparison.currentEnd) : "",
      periodoAnteriorInicio: comparison.previousStart ? clFormatBR(comparison.previousStart) : "",
      periodoAnteriorFim: comparison.previousEnd ? clFormatBR(comparison.previousEnd) : "",
      variacao: clPercentChange(total, anterior),
      ultimaAtualizacao: clGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/checklist-resumo:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== GRAFICOS ==================

app.get("/api/checklist-graficos", requireAuth, async (req, res) => {
  try {
    const rows = await clLoadWithCache();

    const filteredRows = clApplyFilters(rows, req.query);
    const groups = clGroupByChecklist(filteredRows);

    const { groups: periodGroups, period } = clFilterGroupsByPeriod(groups, req.query);

    const porDia = {};
    for (let i = 1; i <= 31; i++) porDia[i] = 0;

    const unidade = {};
    const topico = {};
    const prazo = {};
    const elaborador = {};
    const perguntasErro = {};

    periodGroups.forEach((group) => {
      if (group._dateObj) {
        const day = group._dateObj.getDate();
        porDia[day] = (porDia[day] || 0) + 1;
      }

      const unidadeKey = clNorm(group.unidade) || "Sem unidade";
      unidade[unidadeKey] = (unidade[unidadeKey] || 0) + 1;

      const elaboradorKey = clNorm(group.elaborado_por) || "Sem elaborador";
      elaborador[elaboradorKey] = (elaborador[elaboradorKey] || 0) + 1;

      group.itens.forEach((row) => {
        const topicoKey = clNorm(row["topico"]) || "Sem tópico";
        topico[topicoKey] = (topico[topicoKey] || 0) + 1;

        const prazoKey = clNorm(row["situacao_prazo"]) || "Sem prazo";
        prazo[prazoKey] = (prazo[prazoKey] || 0) + 1;

        const geraNc = clNormLower(row["gera_nc"]);
        const isNc = geraNc === "sim" || geraNc === "s" || geraNc === "yes" || geraNc === "true";

        if (isNc) {
          const pergunta = clNorm(row["pergunta_texto"]) || "Sem pergunta";
          perguntasErro[pergunta] = (perguntasErro[pergunta] || 0) + 1;
        }
      });
    });

    const comparison = clGetComparisonWindow(period.start, period.end);

    const baseNoPeriodRows = clApplyFiltersWithoutPeriod(rows, req.query);
    const baseNoPeriodGroups = clGroupByChecklist(baseNoPeriodRows);

    const mesAtual = baseNoPeriodGroups.filter((g) => {
      const dt = g._dateObj;
      return (
        dt &&
        comparison.currentStart &&
        comparison.currentEnd &&
        dt >= comparison.currentStart &&
        dt <= comparison.currentEnd
      );
    }).length;

    const mesAnterior = baseNoPeriodGroups.filter((g) => {
      const dt = g._dateObj;
      return (
        dt &&
        comparison.previousStart &&
        comparison.previousEnd &&
        dt >= comparison.previousStart &&
        dt <= comparison.previousEnd
      );
    }).length;

    return res.json({
      porDia,
      unidade,
      topico,
      prazo,
      elaborador,
      perguntasErro: Object.entries(perguntasErro)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10),
      comparativoMensal: {
        mesAtual,
        mesAnterior,
        label: comparison.label || "base anterior",
      },
      ultimaAtualizacao: clGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/checklist-graficos:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== DETALHES ==================

app.get("/api/checklist-detalhes", requireAuth, async (req, res) => {
  try {
    const rows = await clLoadWithCache();

    const filteredRows = clApplyFilters(rows, req.query);
    const groups = clGroupByChecklist(filteredRows);

    const { groups: periodGroups } = clFilterGroupsByPeriod(groups, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 25), 1), 200);

    const sorted = [...periodGroups].sort((a, b) => {
      const ad = a._dateObj ? a._dateObj.getTime() : 0;
      const bd = b._dateObj ? b._dateObj.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    const rowsOut = sorted.slice(start, start + limit).map((group) => ({
      registroId: group.registro_id,
      data: group._dateObj ? clFormatBR(group._dateObj) : group.data_checklist || "",
      elaborador: group.elaborado_por || "",
      unidade: group.unidade || "",
      funcao: group.funcao || "",
      estado: group.estado || "",
      cidade: group.cidade || "",
      totalItens: group.totalItens,
      totalNc: group.totalNc,
      pendencias: group.pendencias,
      conformidade: group.conforme ? "Conforme" : "Com NC",
      mediaPontuacao: Number(group.mediaPontuacao || 0).toFixed(1),
    }));

    return res.json({
      total,
      page: currentPage,
      limit,
      totalPages,
      rows: rowsOut,
      ultimaAtualizacao: clGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/checklist-detalhes:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== DEBUG ==================

app.get("/api/checklist-debug", requireAuth, async (req, res) => {
  try {
    const rows = await clLoadWithCache();

    return res.json({
      totalLinhas: rows.length,
      headers: rows.length ? Object.keys(rows[0]).filter((h) => !h.startsWith("_")) : [],
      sample: rows.slice(0, 5),
      ultimaAtualizacao: clGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/checklist-debug:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== PRESENTEISMO DASHBOARD ==================

app.get("/presenteismo", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "presenteismo.html"));
});

const PRESENTEISMO_SPREADSHEET_ID = "1DivjZdxzDES6Nu_ZJI_1gmrJgKqlrlqYgg3I-ybPr5k";
const PRESENTEISMO_RANGE = "'REGISTRO DE PRESENTEÍSMO'!A1:L200000";

let presenteismoCache = null;
let presenteismoCacheTime = 0;
const PRESENTEISMO_TTL = 5 * 60 * 1000;

// ================== UTIL ==================

function n(v) {
  return String(v || "").trim();
}

function nLower(v) {
  return n(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function parseDate(v) {
  const m = n(v).match(/^(\d{2})\/(\d{2})\/(\d{4})/);
  if (!m) return null;
  return new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
}

function parseHours(v) {
  const p = n(v).split(":").map(Number);
  if (p.length !== 3 || p.some(isNaN)) return 0;
  return p[0] + (p[1] / 60) + (p[2] / 3600);
}

function parseMoney(v) {
  return Number(
    n(v)
      .replace(/[R$\s]/g, "")
      .replace(/\./g, "")
      .replace(",", ".")
  ) || 0;
}

function formatBR(date) {
  if (!date) return "";
  const dd = String(date.getDate()).padStart(2, "0");
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const yyyy = date.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function getHoraAtualizacaoBR() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function startOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function endOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function diffDaysInclusive(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.floor((startOfDay(end) - startOfDay(start)) / msPerDay) + 1;
}

function splitMulti(value) {
  if (!value) return [];
  return String(value).split("|").map((v) => n(v)).filter(Boolean);
}

function matchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(n(fieldValue));
}

// ================== PERÍODO ==================

function resolvePeriodFromQuery(query, rows) {
  const periodoTipo = nLower(query.periodoTipo || "mes");
  const dataRef = n(query.dataRef);

  if (dataRef) {
    const ref = new Date(`${dataRef}T00:00:00`);

    if (!isNaN(ref.getTime())) {
      if (periodoTipo === "dia") {
        return {
          start: startOfDay(ref),
          end: endOfDay(ref),
        };
      }

      if (periodoTipo === "semana") {
        const weekStart = new Date(ref);
        const day = weekStart.getDay();
        const diff = day === 0 ? -6 : 1 - day;
        weekStart.setDate(weekStart.getDate() + diff);

        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        return {
          start: startOfDay(weekStart),
          end: endOfDay(weekEnd),
        };
      }

      if (periodoTipo === "ano") {
        return {
          start: new Date(ref.getFullYear(), 0, 1, 0, 0, 0, 0),
          end: new Date(ref.getFullYear(), 11, 31, 23, 59, 59, 999),
        };
      }

      return {
        start: new Date(ref.getFullYear(), ref.getMonth(), 1, 0, 0, 0, 0),
        end: new Date(ref.getFullYear(), ref.getMonth() + 1, 0, 23, 59, 59, 999),
      };
    }
  }

  const validDates = rows
    .map((r) => r._date)
    .filter(Boolean)
    .sort((a, b) => a - b);

  if (!validDates.length) {
    return { start: null, end: null };
  }

  return {
    start: startOfDay(validDates[0]),
    end: endOfDay(validDates[validDates.length - 1]),
  };
}

function getComparisonWindow(startDate, endDate) {
  if (!startDate || !endDate) {
    return {
      currentStart: null,
      currentEnd: null,
      previousStart: null,
      previousEnd: null,
      label: "base anterior",
    };
  }

  const start = startOfDay(startDate);
  const end = endOfDay(endDate);

  const isSingleDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();

  const isFullMonth =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === 1 &&
    end.getDate() === new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();

  const isFullYear =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === 0 &&
    start.getDate() === 1 &&
    end.getMonth() === 11 &&
    end.getDate() === 31;

  const totalDays = diffDaysInclusive(start, end);

  if (isSingleDay) {
    const prev = new Date(start);
    prev.setDate(prev.getDate() - 1);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart: startOfDay(prev),
      previousEnd: endOfDay(prev),
      label: "dia anterior",
    };
  }

  if (isFullMonth) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear(), start.getMonth() - 1, 1),
      previousEnd: new Date(start.getFullYear(), start.getMonth(), 0, 23, 59, 59, 999),
      label: "mês anterior",
    };
  }

  if (isFullYear) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear() - 1, 0, 1),
      previousEnd: new Date(start.getFullYear() - 1, 11, 31, 23, 59, 59, 999),
      label: "ano anterior",
    };
  }

  const previousEnd = new Date(start);
  previousEnd.setDate(previousEnd.getDate() - 1);
  previousEnd.setHours(23, 59, 59, 999);

  const previousStart = new Date(previousEnd);
  previousStart.setDate(previousStart.getDate() - (totalDays - 1));
  previousStart.setHours(0, 0, 0, 0);

  return {
    currentStart: start,
    currentEnd: end,
    previousStart,
    previousEnd,
    label: totalDays === 7 ? "semana anterior" : `período anterior (${totalDays} dias)`,
  };
}

// ================== LOAD ==================

async function loadData() {
  const now = Date.now();
  if (presenteismoCache && now - presenteismoCacheTime < PRESENTEISMO_TTL) {
    return presenteismoCache;
  }

  const sheets = await conectarSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: PRESENTEISMO_SPREADSHEET_ID,
    range: PRESENTEISMO_RANGE,
  });

  const values = res.data.values || [];
  if (!values.length || values.length < 2) {
    presenteismoCache = [];
    presenteismoCacheTime = now;
    return presenteismoCache;
  }

  const headers = values[0].map((h, i) => n(h) || `COL_${i + 1}`);
  const filldownColumns = [
    "MÊS",
    "DATA",
    "UNIDADE",
    "EMPRESA",
    "PLANTÃO",
    "TURNO",
    "AGENTE",
    "STATUS",
    "STATUS DE COBERTURA",
    "ABSENTEISMO",
    "DESCONTO",
    "COLABORADOR COBRINDO POSTO"
  ];

  const lastSeen = {};

  const data = values.slice(1).map((r) => {
    const obj = {};

    headers.forEach((h, i) => {
      let value = r[i] || "";

      if (!n(value) && filldownColumns.includes(h) && lastSeen[h] !== undefined) {
        value = lastSeen[h];
      }

      if (n(value)) {
        lastSeen[h] = value;
      }

      obj[h] = value;
    });

    obj._date = parseDate(obj["DATA"]);
    obj._day = obj._date ? obj._date.getDate() : null;
    obj._abs = parseHours(obj["ABSENTEISMO"]);
    obj._desconto = parseMoney(obj["DESCONTO"]);

    return obj;
  }).filter((row) =>
    n(row["DATA"]) ||
    n(row["PLANTÃO"]) ||
    n(row["AGENTE"]) ||
    n(row["STATUS DE COBERTURA"])
  );

  presenteismoCache = data;
  presenteismoCacheTime = now;

  return data;
}

// ================== FILTROS ==================

function applyFilters(rows, query) {
  const unidade = splitMulti(query.unidade);
  const empresa = splitMulti(query.empresa);
  const plantao = splitMulti(query.plantao);
  const turno = splitMulti(query.turno);
  const agente = splitMulti(query.agente);
  const status = splitMulti(query.status);
  const cobertura = splitMulti(query.cobertura);
  const busca = nLower(query.busca);

  return rows.filter((r) => {
    if (!matchesMulti(r["UNIDADE"], unidade)) return false;
    if (!matchesMulti(r["EMPRESA"], empresa)) return false;
    if (!matchesMulti(r["PLANTÃO"], plantao)) return false;
    if (!matchesMulti(r["TURNO"], turno)) return false;
    if (!matchesMulti(r["AGENTE"], agente)) return false;
    if (!matchesMulti(r["STATUS"], status)) return false;
    if (!matchesMulti(r["STATUS DE COBERTURA"], cobertura)) return false;

    if (busca) {
      const text = Object.values(r)
        .filter((v) => typeof v !== "object")
        .join(" ")
        .toLowerCase();

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function applyFiltersWithoutPeriod(rows, query) {
  return applyFilters(rows, {
    ...query,
    periodoTipo: "",
    dataRef: "",
  });
}

function filterByPeriod(rows, query) {
  const period = resolvePeriodFromQuery(query, rows);

  if (!period.start || !period.end) {
    return {
      rows,
      period,
    };
  }

  return {
    rows: rows.filter((r) => r._date && r._date >= period.start && r._date <= period.end),
    period,
  };
}

// ================== FILTROS API ==================

app.get("/api/presenteismo-filtros", requireAuth, async (req, res) => {
  try {
    const rows = await loadData();

    const uniq = (f) =>
      [...new Set(rows.map((r) => n(r[f])).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json({
      unidade: uniq("UNIDADE"),
      empresa: uniq("EMPRESA"),
      plantao: uniq("PLANTÃO"),
      turno: uniq("TURNO"),
      agente: uniq("AGENTE"),
      status: uniq("STATUS"),
      cobertura: uniq("STATUS DE COBERTURA"),
    });
  } catch (error) {
    console.error("Erro /api/presenteismo-filtros:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== RESUMO API ==================

app.get("/api/presenteismo-resumo", requireAuth, async (req, res) => {
  try {
    const rows = await loadData();

    const filteredRows = applyFilters(rows, req.query);
    const { rows: periodRows, period } = filterByPeriod(filteredRows, req.query);

    const total = periodRows.length;
    const horasAbs = periodRows.reduce((sum, row) => sum + Number(row._abs || 0), 0);
    const descontoTotal = periodRows.reduce((sum, row) => sum + Number(row._desconto || 0), 0);

    const registrosComAbs = periodRows.filter((row) => Number(row._abs || 0) > 0).length;
    const percentualAbsenteismo = total ? (registrosComAbs / total) * 100 : 0;

    const coberturaIntegral = periodRows.filter((row) =>
      nLower(row["STATUS DE COBERTURA"]).includes("integral")
    ).length;

    const coberturaParcial = periodRows.filter((row) =>
      nLower(row["STATUS DE COBERTURA"]).includes("parcial")
    ).length;

    const semCobertura = periodRows.filter((row) => {
      const c = nLower(row["STATUS DE COBERTURA"]);
      return !c || c.includes("sem cobertura") || c.includes("nao coberto") || c.includes("não coberto");
    }).length;

    const comparison = getComparisonWindow(period.start, period.end);
    const baseNoPeriod = applyFiltersWithoutPeriod(rows, req.query);

    const anteriorRows = baseNoPeriod.filter((row) => {
      const dt = row._date;
      return (
        dt &&
        comparison.previousStart &&
        comparison.previousEnd &&
        dt >= comparison.previousStart &&
        dt <= comparison.previousEnd
      );
    });

    const anteriorHoras = anteriorRows.reduce((sum, row) => sum + Number(row._abs || 0), 0);

    return res.json({
      total,
      horasAbs,
      descontoTotal,
      registrosComAbs,
      percentualAbsenteismo,
      coberturaIntegral,
      coberturaParcial,
      semCobertura,
      anteriorHoras,
      comparativoLabel: comparison.label || "base anterior",
      periodoAtualInicio: comparison.currentStart ? formatBR(comparison.currentStart) : "",
      periodoAtualFim: comparison.currentEnd ? formatBR(comparison.currentEnd) : "",
      periodoAnteriorInicio: comparison.previousStart ? formatBR(comparison.previousStart) : "",
      periodoAnteriorFim: comparison.previousEnd ? formatBR(comparison.previousEnd) : "",
      ultimaAtualizacao: getHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/presenteismo-resumo:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== GRAFICOS API ==================

app.get("/api/presenteismo-graficos", requireAuth, async (req, res) => {
  try {
    const rows = await loadData();

    const filteredRows = applyFilters(rows, req.query);
    const { rows: periodRows, period } = filterByPeriod(filteredRows, req.query);

    let empresa = {};
    let plantao = {};
    let unidade = {};
    let cobertura = {};
    let dia = {};
    let agente = {};
    let desconto = {};

    for (let i = 1; i <= 31; i++) dia[i] = 0;

    periodRows.forEach((r) => {
      const p = n(r["PLANTÃO"]) || "SEM";
      const u = n(r["UNIDADE"]) || "SEM";
      const c = n(r["STATUS DE COBERTURA"]) || "SEM";
      const a = n(r["AGENTE"]) || "SEM";
      const e = n(r["EMPRESA"]) || "SEM";

      plantao[p] = (plantao[p] || 0) + Number(r._abs || 0);
      unidade[u] = (unidade[u] || 0) + 1;
      cobertura[c] = (cobertura[c] || 0) + 1;
      agente[a] = (agente[a] || 0) + Number(r._abs || 0);
      desconto[p] = (desconto[p] || 0) + Number(r._desconto || 0);
      empresa[e] = (empresa[e] || 0) + 1;

      if (r._day) dia[r._day] += Number(r._abs || 0);
    });

    const top = (obj, limit = 10) =>
      Object.fromEntries(Object.entries(obj).sort((a, b) => b[1] - a[1]).slice(0, limit));

    const comparison = getComparisonWindow(period.start, period.end);
    const baseNoPeriod = applyFiltersWithoutPeriod(rows, req.query);

    const anterior = baseNoPeriod
      .filter((row) => {
        const dt = row._date;
        return (
          dt &&
          comparison.previousStart &&
          comparison.previousEnd &&
          dt >= comparison.previousStart &&
          dt <= comparison.previousEnd
        );
      })
      .reduce((sum, row) => sum + Number(row._abs || 0), 0);

    const atual = periodRows.reduce((sum, row) => sum + Number(row._abs || 0), 0);

    return res.json({
      horasPorPlantao: top(plantao, 12),
      empresas: top(empresa, 12),
      registrosPorUnidade: top(unidade, 12),
      cobertura: top(cobertura, 10),
      horasPorDia: dia,
      horasPorAgente: top(agente, 12),
      descontoPorPlantao: top(desconto, 12),
      comparativo: {
        atual,
        anterior,
        label: comparison.label || "base anterior",
      }
    });
  } catch (error) {
    console.error("Erro /api/presenteismo-graficos:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== DETALHES API ==================

app.get("/api/presenteismo-detalhes", requireAuth, async (req, res) => {
  try {
    const rows = await loadData();

    const filteredRows = applyFilters(rows, req.query);
    const { rows: periodRows } = filterByPeriod(filteredRows, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 20), 1), 200);

    const sorted = [...periodRows].sort((a, b) => {
      const ad = a._date ? a._date.getTime() : 0;
      const bd = b._date ? b._date.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    return res.json({
      page: currentPage,
      totalPages,
      rows: sorted.slice(start, start + limit).map((r) => ({
        mes: r["MÊS"] || "",
        data: r["DATA"] || "",
        unidade: r["UNIDADE"] || "",
        empresa: r["EMPRESA"] || "",
        plantao: r["PLANTÃO"] || "",
        turno: r["TURNO"] || "",
        agente: r["AGENTE"] || "",
        status: r["STATUS"] || "",
        statusCobertura: r["STATUS DE COBERTURA"] || "",
        abs: r["ABSENTEISMO"] || "",
        desconto: r["DESCONTO"] || "",
        cobrindo: r["COLABORADOR COBRINDO POSTO"] || "",
      })),
    });
  } catch (error) {
    console.error("Erro /api/presenteismo-detalhes:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== HC FBS DASHBOARD ==================

app.get("/hc-fbs", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "hc-fbs.html"));
});

const HCFBS_SPREADSHEET_ID = "1s7ZG9N7pS0pafmYb-4QfQXZPily21gp16_-Vzdy7m5I";
const HCFBS_RANGE = "A:H";

let hcfbsCache = null;
let hcfbsCacheTime = 0;
const HCFBS_CACHE_TTL = 5 * 60 * 1000;

function hcNorm(v) {
  return String(v || "").trim();
}

function hcNormLower(v) {
  return hcNorm(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function hcGetHoraAtualizacaoBR() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function hcParseDate(value) {
  const str = hcNorm(value);
  if (!str) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(str)) {
    const [d, m, y] = str.split("/").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  return null;
}

function hcFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function hcSplitMulti(value) {
  if (!value) return [];
  return String(value).split("|").map(v => hcNorm(v)).filter(Boolean);
}

function hcMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(hcNorm(fieldValue));
}

function hcPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);

  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;

  return ((current - previous) / previous) * 100;
}

function hcStartOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function hcEndOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function hcDiffDaysInclusive(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const s = hcStartOfDay(start);
  const e = hcStartOfDay(end);
  return Math.floor((e - s) / msPerDay) + 1;
}

function hcResolvePeriodFromQuery(query, rows) {
  const periodoTipo = hcNormLower(query.periodoTipo || "mes");
  const dataRef = hcNorm(query.dataRef);

  if (dataRef) {
    const ref = new Date(`${dataRef}T00:00:00`);

    if (!isNaN(ref.getTime())) {
      if (periodoTipo === "dia") {
        return {
          start: hcStartOfDay(ref),
          end: hcEndOfDay(ref),
        };
      }

      if (periodoTipo === "semana") {
        const weekStart = new Date(ref);
        const day = weekStart.getDay();
        const diff = day === 0 ? -6 : 1 - day;
        weekStart.setDate(weekStart.getDate() + diff);

        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        return {
          start: hcStartOfDay(weekStart),
          end: hcEndOfDay(weekEnd),
        };
      }

      if (periodoTipo === "ano") {
        return {
          start: new Date(ref.getFullYear(), 0, 1),
          end: new Date(ref.getFullYear(), 11, 31, 23, 59, 59, 999),
        };
      }

      return {
        start: new Date(ref.getFullYear(), ref.getMonth(), 1),
        end: new Date(ref.getFullYear(), ref.getMonth() + 1, 0, 23, 59, 59, 999),
      };
    }
  }

  const validDates = rows
    .map((r) => r._dateObj)
    .filter(Boolean)
    .sort((a, b) => a - b);

  if (!validDates.length) return { start: null, end: null };

  const latest = validDates[validDates.length - 1];

  return {
    start: new Date(latest.getFullYear(), latest.getMonth(), 1),
    end: new Date(latest.getFullYear(), latest.getMonth() + 1, 0, 23, 59, 59, 999),
  };
}

function hcGetComparisonWindow(startDate, endDate) {
  if (!startDate || !endDate) {
    return {
      currentStart: null,
      currentEnd: null,
      previousStart: null,
      previousEnd: null,
      label: "base anterior",
    };
  }

  const start = hcStartOfDay(startDate);
  const end = hcEndOfDay(endDate);

  const isSingleDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();

  const isFullMonth =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === 1 &&
    end.getDate() === new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();

  const isFullYear =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === 0 &&
    start.getDate() === 1 &&
    end.getMonth() === 11 &&
    end.getDate() === 31;

  const totalDays = hcDiffDaysInclusive(start, end);

  if (isSingleDay) {
    const prev = new Date(start);
    prev.setDate(prev.getDate() - 1);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart: hcStartOfDay(prev),
      previousEnd: hcEndOfDay(prev),
      label: "dia anterior",
    };
  }

  if (isFullMonth) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear(), start.getMonth() - 1, 1),
      previousEnd: new Date(start.getFullYear(), start.getMonth(), 0, 23, 59, 59, 999),
      label: "mês anterior",
    };
  }

  if (isFullYear) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear() - 1, 0, 1),
      previousEnd: new Date(start.getFullYear() - 1, 11, 31, 23, 59, 59, 999),
      label: "ano anterior",
    };
  }

  const previousEnd = new Date(start);
  previousEnd.setDate(previousEnd.getDate() - 1);
  previousEnd.setHours(23, 59, 59, 999);

  const previousStart = new Date(previousEnd);
  previousStart.setDate(previousStart.getDate() - (totalDays - 1));
  previousStart.setHours(0, 0, 0, 0);

  return {
    currentStart: start,
    currentEnd: end,
    previousStart,
    previousEnd,
    label: totalDays === 7 ? "semana anterior" : `período anterior (${totalDays} dias)`,
  };
}

async function hcLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: HCFBS_SPREADSHEET_ID,
    range: HCFBS_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const rows = values.slice(1);

  return rows.map((line) => {
    const obj = {
      data: line[0] ?? "",
      bpo: line[1] ?? "",
      qtd_admitidos: Number(line[2] || 0),
      qtd_desligados: line[3] ?? "",
      num_semana: line[4] ?? "",
      num_mes: line[5] ?? "",
      turno_g: line[6] ?? "",
      turno: line[7] ?? "", // coluna H
    };

    obj._dateObj = hcParseDate(obj.data);
    obj._day = obj._dateObj ? obj._dateObj.getDate() : null;
    obj._year = obj._dateObj ? obj._dateObj.getFullYear() : null;
    obj._month = obj._dateObj ? obj._dateObj.getMonth() + 1 : null;

    return obj;
  });
}

async function hcLoadWithCache() {
  const now = Date.now();

  if (hcfbsCache && now - hcfbsCacheTime < HCFBS_CACHE_TTL) {
    return hcfbsCache;
  }

  const rows = await hcLoadRaw();
  hcfbsCache = rows;
  hcfbsCacheTime = now;

  console.log("HC FBS cache atualizado:", rows.length);

  return hcfbsCache;
}

function hcApplyFilters(rows, query) {
  const bpo = hcSplitMulti(query.bpo);
  const turno = hcSplitMulti(query.turno);
  const ano = hcSplitMulti(query.ano);
  const mes = hcSplitMulti(query.mes);
  const semana = hcSplitMulti(query.semana);
  const dia = hcSplitMulti(query.dia);
  const busca = hcNormLower(query.busca);

  return rows.filter((row) => {
    if (!hcMatchesMulti(row.bpo, bpo)) return false;
    if (!hcMatchesMulti(row.turno, turno)) return false;
    if (ano.length && !ano.includes(String(row._year || ""))) return false;
    if (mes.length && !mes.includes(String(row._month || ""))) return false;
    if (semana.length && !semana.includes(String(row.num_semana || ""))) return false;
    if (dia.length && !dia.includes(String(row._day || ""))) return false;

    if (busca) {
      const text = Object.values(row)
        .filter((v) => typeof v !== "object")
        .join(" ")
        .toLowerCase();

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function hcApplyFiltersWithoutPeriod(rows, query) {
  return hcApplyFilters(rows, {
    ...query,
    periodoTipo: "",
    dataRef: "",
  });
}

function hcFilterRowsByPeriod(rows, query) {
  const period = hcResolvePeriodFromQuery(query, rows);

  if (!period.start || !period.end) {
    return { rows, period };
  }

  const filtered = rows.filter((row) => {
    const dt = row._dateObj;
    return dt && dt >= period.start && dt <= period.end;
  });

  return { rows: filtered, period };
}

app.get("/api/hc-fbs-filtros", requireAuth, async (req, res) => {
  try {
    const rows = await hcLoadWithCache();

    const uniq = (getter) =>
      [...new Set(rows.map(getter).map(v => hcNorm(v)).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const uniqNum = (getter) =>
      [...new Set(rows.map(getter).map(v => String(v || "")).filter(Boolean))]
        .sort((a, b) => Number(a) - Number(b));

    return res.json({
      bpo: uniq(r => r.bpo),
      turno: uniq(r => r.turno),
      ano: uniqNum(r => r._year),
      mes: uniqNum(r => r._month),
      semana: uniqNum(r => r.num_semana),
      dia: uniqNum(r => r._day),
      ultimaAtualizacao: hcGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/hc-fbs-filtros:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/hc-fbs-resumo", requireAuth, async (req, res) => {
  try {
    const rows = await hcLoadWithCache();

    const filteredRows = hcApplyFilters(rows, req.query);
    const { rows: periodRows, period } = hcFilterRowsByPeriod(filteredRows, req.query);

    const totalAdmitidos = periodRows.reduce((sum, row) => sum + Number(row.qtd_admitidos || 0), 0);
    const totalRegistros = periodRows.length;
    const totalBpos = new Set(periodRows.map(r => hcNorm(r.bpo)).filter(Boolean)).size;
    const totalTurnos = new Set(periodRows.map(r => hcNorm(r.turno)).filter(Boolean)).size;

    const comparison = hcGetComparisonWindow(period.start, period.end);
    const baseNoPeriodRows = hcApplyFiltersWithoutPeriod(rows, req.query);

    const previousRows = baseNoPeriodRows.filter((row) => {
      const dt = row._dateObj;
      return (
        dt &&
        comparison.previousStart &&
        comparison.previousEnd &&
        dt >= comparison.previousStart &&
        dt <= comparison.previousEnd
      );
    });

    const admitidosAnterior = previousRows.reduce((sum, row) => sum + Number(row.qtd_admitidos || 0), 0);

    return res.json({
      totalAdmitidos,
      totalRegistros,
      totalBpos,
      totalTurnos,
      admitidosAnterior,
      variacaoAdmitidos: hcPercentChange(totalAdmitidos, admitidosAnterior),
      comparativoLabel: comparison.label || "base anterior",
      periodoAtualInicio: comparison.currentStart ? hcFormatBR(comparison.currentStart) : "",
      periodoAtualFim: comparison.currentEnd ? hcFormatBR(comparison.currentEnd) : "",
      periodoAnteriorInicio: comparison.previousStart ? hcFormatBR(comparison.previousStart) : "",
      periodoAnteriorFim: comparison.previousEnd ? hcFormatBR(comparison.previousEnd) : "",
      ultimaAtualizacao: hcGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/hc-fbs-resumo:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/hc-fbs-graficos", requireAuth, async (req, res) => {
  try {
    const rows = await hcLoadWithCache();

    const filteredRows = hcApplyFilters(rows, req.query);
    const { rows: periodRows, period } = hcFilterRowsByPeriod(filteredRows, req.query);

    const porDia = {};
    for (let i = 1; i <= 31; i++) porDia[i] = 0;

    const porBpo = {};
    const porTurno = {};
    const porSemana = {};

    periodRows.forEach((row) => {
      const qtd = Number(row.qtd_admitidos || 0);

      if (row._day) porDia[row._day] += qtd;

      const bpo = hcNorm(row.bpo) || "Sem BPO";
      porBpo[bpo] = (porBpo[bpo] || 0) + qtd;

      const turno = hcNorm(row.turno) || "Sem Turno";
      porTurno[turno] = (porTurno[turno] || 0) + qtd;

      const semana = `Semana ${row.num_semana || "?"}`;
      porSemana[semana] = (porSemana[semana] || 0) + qtd;
    });

    const comparison = hcGetComparisonWindow(period.start, period.end);
    const baseNoPeriodRows = hcApplyFiltersWithoutPeriod(rows, req.query);

    const atual = periodRows.reduce((sum, row) => sum + Number(row.qtd_admitidos || 0), 0);

    const anterior = baseNoPeriodRows
      .filter((row) => {
        const dt = row._dateObj;
        return (
          dt &&
          comparison.previousStart &&
          comparison.previousEnd &&
          dt >= comparison.previousStart &&
          dt <= comparison.previousEnd
        );
      })
      .reduce((sum, row) => sum + Number(row.qtd_admitidos || 0), 0);

    return res.json({
      porDia,
      porBpo,
      porTurno,
      porSemana,
      comparativo: {
        atual,
        anterior,
        label: comparison.label || "base anterior",
      },
      ultimaAtualizacao: hcGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/hc-fbs-graficos:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/hc-fbs-detalhes", requireAuth, async (req, res) => {
  try {
    const rows = await hcLoadWithCache();

    const filteredRows = hcApplyFilters(rows, req.query);
    const { rows: periodRows } = hcFilterRowsByPeriod(filteredRows, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 200);

    const sorted = [...periodRows].sort((a, b) => {
      const ad = a._dateObj ? a._dateObj.getTime() : 0;
      const bd = b._dateObj ? b._dateObj.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    const rowsOut = sorted.slice(start, start + limit).map((row) => ({
      data: row._dateObj ? hcFormatBR(row._dateObj) : row.data || "",
      bpo: row.bpo || "",
      qtdAdmitidos: Number(row.qtd_admitidos || 0),
      semana: row.num_semana || "",
      mes: row.num_mes || "",
      turno: row.turno || "",
    }));

    return res.json({
      total,
      page: currentPage,
      limit,
      totalPages,
      rows: rowsOut,
      ultimaAtualizacao: hcGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/hc-fbs-detalhes:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== FALTAS ALTUM DASHBOARD ==================

app.get("/faltas-altum", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "faltas-altum.html"));
});

const FALTAS_ALTUM_SPREADSHEET_ID = "1XtP5ylCpA42aLE1EklytzHH2hUzik5JdY8meAg84Et4";
const FALTAS_ALTUM_RANGE = "'CONTROLE DE FALTAS FBS'!A:M";

let faltasAltumCache = null;
let faltasAltumCacheTime = 0;
const FALTAS_ALTUM_CACHE_TTL = 5 * 60 * 1000;

// Base fixa de efetivo
const FA_BASE_EFETIVO = {
  "FBS - SP9": { DIURNO: 22, NOTURNO: 22 },
  "FBS - SP20": { DIURNO: 12, NOTURNO: 12 },
  "FBS - MG5": { DIURNO: 10, NOTURNO: 10 },
  "FBS - PE3": { DIURNO: 19, NOTURNO: 19 },
  "FBS - GO3": { DIURNO: 13, NOTURNO: 13 },
  "FBS - RS3": { DIURNO: 8, NOTURNO: 8 },
};

function faNorm(v) {
  return String(v || "").trim();
}

function faNormLower(v) {
  return faNorm(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function faGetHoraAtualizacaoBR() {
  return new Date().toLocaleTimeString("pt-BR", {
    timeZone: "America/Sao_Paulo",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function faParseDate(value) {
  const str = faNorm(value);
  if (!str) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
    const [y, m, d] = str.split("-").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(str)) {
    const [d, m, y] = str.split("/").map(Number);
    const dt = new Date(y, m - 1, d);
    return isNaN(dt.getTime()) ? null : dt;
  }

  return null;
}

function faFormatBR(date) {
  if (!date) return "";
  const d = String(date.getDate()).padStart(2, "0");
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const y = date.getFullYear();
  return `${d}/${m}/${y}`;
}

function faSplitMulti(value) {
  if (!value) return [];
  return String(value).split("|").map(v => faNorm(v)).filter(Boolean);
}

function faMatchesMulti(fieldValue, selectedValues) {
  if (!selectedValues.length) return true;
  return selectedValues.includes(faNorm(fieldValue));
}

function faPercentChange(current, previous) {
  current = Number(current || 0);
  previous = Number(previous || 0);

  if (previous === 0 && current === 0) return 0;
  if (previous === 0) return 100;

  return ((current - previous) / previous) * 100;
}

function faStartOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
}

function faEndOfDay(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 23, 59, 59, 999);
}

function faDiffDaysInclusive(start, end) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const s = faStartOfDay(start);
  const e = faStartOfDay(end);
  return Math.floor((e - s) / msPerDay) + 1;
}

function faResolvePeriodFromQuery(query, rows) {
  const periodoTipo = faNormLower(query.periodoTipo || "mes");
  const dataRef = faNorm(query.dataRef);

  if (dataRef) {
    const ref = new Date(`${dataRef}T00:00:00`);

    if (!isNaN(ref.getTime())) {
      if (periodoTipo === "dia") {
        return { start: faStartOfDay(ref), end: faEndOfDay(ref) };
      }

      if (periodoTipo === "semana") {
        const weekStart = new Date(ref);
        const day = weekStart.getDay();
        const diff = day === 0 ? -6 : 1 - day;
        weekStart.setDate(weekStart.getDate() + diff);

        const weekEnd = new Date(weekStart);
        weekEnd.setDate(weekEnd.getDate() + 6);

        return { start: faStartOfDay(weekStart), end: faEndOfDay(weekEnd) };
      }

      if (periodoTipo === "ano") {
        return {
          start: new Date(ref.getFullYear(), 0, 1),
          end: new Date(ref.getFullYear(), 11, 31, 23, 59, 59, 999),
        };
      }

      return {
        start: new Date(ref.getFullYear(), ref.getMonth(), 1),
        end: new Date(ref.getFullYear(), ref.getMonth() + 1, 0, 23, 59, 59, 999),
      };
    }
  }

  const validDates = rows.map(r => r._dateObj).filter(Boolean).sort((a, b) => a - b);
  if (!validDates.length) return { start: null, end: null };

  const latest = validDates[validDates.length - 1];
  return {
    start: new Date(latest.getFullYear(), latest.getMonth(), 1),
    end: new Date(latest.getFullYear(), latest.getMonth() + 1, 0, 23, 59, 59, 999),
  };
}

function faGetComparisonWindow(startDate, endDate) {
  if (!startDate || !endDate) {
    return {
      currentStart: null,
      currentEnd: null,
      previousStart: null,
      previousEnd: null,
      label: "base anterior",
    };
  }

  const start = faStartOfDay(startDate);
  const end = faEndOfDay(endDate);

  const isSingleDay =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === end.getDate();

  const isFullMonth =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === end.getMonth() &&
    start.getDate() === 1 &&
    end.getDate() === new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate();

  const isFullYear =
    start.getFullYear() === end.getFullYear() &&
    start.getMonth() === 0 &&
    start.getDate() === 1 &&
    end.getMonth() === 11 &&
    end.getDate() === 31;

  const totalDays = faDiffDaysInclusive(start, end);

  if (isSingleDay) {
    const prev = new Date(start);
    prev.setDate(prev.getDate() - 1);

    return {
      currentStart: start,
      currentEnd: end,
      previousStart: faStartOfDay(prev),
      previousEnd: faEndOfDay(prev),
      label: "dia anterior",
    };
  }

  if (isFullMonth) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear(), start.getMonth() - 1, 1),
      previousEnd: new Date(start.getFullYear(), start.getMonth(), 0, 23, 59, 59, 999),
      label: "mês anterior",
    };
  }

  if (isFullYear) {
    return {
      currentStart: start,
      currentEnd: end,
      previousStart: new Date(start.getFullYear() - 1, 0, 1),
      previousEnd: new Date(start.getFullYear() - 1, 11, 31, 23, 59, 59, 999),
      label: "ano anterior",
    };
  }

  const previousEnd = new Date(start);
  previousEnd.setDate(previousEnd.getDate() - 1);
  previousEnd.setHours(23, 59, 59, 999);

  const previousStart = new Date(previousEnd);
  previousStart.setDate(previousStart.getDate() - (totalDays - 1));
  previousStart.setHours(0, 0, 0, 0);

  return {
    currentStart: start,
    currentEnd: end,
    previousStart,
    previousEnd,
    label: totalDays === 7 ? "semana anterior" : `período anterior (${totalDays} dias)`,
  };
}

function faParseHoursToDecimal(value) {
  const str = faNorm(value);
  if (!str || !/^\d{1,2}:\d{2}:\d{2}$/.test(str)) return 0;

  const [h, m, s] = str.split(":").map(Number);
  return h + (m / 60) + (s / 3600);
}

function faFormatHoursDecimal(value) {
  const total = Number(value || 0);
  const hours = Math.floor(total);
  const minutes = Math.round((total - hours) * 60);

  return `${hours}h${String(minutes).padStart(2, "0")}`;
}

function faIsFaltaOuPostoVago(row) {
  const ocorrencia = faNormLower(row.ocorrencia);
  return ocorrencia === "falta" || ocorrencia === "posto vago";
}

function faNormalizeUnidadeBase(unidade) {
  const u = faNormLower(unidade)
    .replace(/\s+/g, " ")
    .replace(/\s*-\s*/g, " - ")
    .toUpperCase();

  const aliases = {
    "FBS - SP9": "FBS - SP9",
    "FBS - SP09": "FBS - SP9",
    "FBS - SP20": "FBS - SP20",
    "FBS - MG5": "FBS - MG5",
    "FBS - MG05": "FBS - MG5",
    "FBS - PE3": "FBS - PE3",
    "FBS - PE03": "FBS - PE3",
    "FBS - GO3": "FBS - GO3",
    "FBS - GO03": "FBS - GO3",
    "FBS - RS3": "FBS - RS3",
    "FBS - RS03": "FBS - RS3",
  };

  return aliases[u] || faNorm(unidade);
}

function faNormalizeTurnoBase(turno) {
  const t = faNormLower(turno);

  if (t.includes("dia")) return "DIURNO";
  if (t.includes("not")) return "NOTURNO";

  return "";
}

function faGetBaseAgentesFromQuery(query) {
  const unidadesSelecionadas = faSplitMulti(query.unidade).map(faNormalizeUnidadeBase);
  const turnosSelecionados = faSplitMulti(query.turno).map(faNormalizeTurnoBase).filter(Boolean);

  const unidadesBase = unidadesSelecionadas.length
    ? unidadesSelecionadas.filter(u => FA_BASE_EFETIVO[u])
    : Object.keys(FA_BASE_EFETIVO);

  const turnosBase = turnosSelecionados.length
    ? turnosSelecionados
    : ["DIURNO", "NOTURNO"];

  let total = 0;

  for (const unidade of unidadesBase) {
    for (const turno of turnosBase) {
      total += Number(FA_BASE_EFETIVO[unidade]?.[turno] || 0);
    }
  }

  return total;
}

async function faLoadRaw() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: FALTAS_ALTUM_SPREADSHEET_ID,
    range: FALTAS_ALTUM_RANGE,
  });

  const values = response.data.values || [];
  if (!values.length || values.length < 2) return [];

  const rows = values.slice(1);

  return rows.map((line) => {
    const obj = {
      mes: line[0] ?? "",
      data: line[1] ?? "",
      unidade: line[2] ?? "",
      plantao: line[3] ?? "",
      turno: line[4] ?? "",
      ocorrencia: line[5] ?? "",
      qtd: Number(line[6] || 0),
      tempoNaoCoberto: line[7] ?? "",
      absenteismo: line[8] ?? "",
      desconto: line[9] ?? "",
      colaborador: line[10] ?? "",
      cobertura: line[11] ?? "",
      pctAbs: line[12] ?? "",
    };

    obj._dateObj = faParseDate(obj.data);
    obj._day = obj._dateObj ? obj._dateObj.getDate() : null;
    obj._year = obj._dateObj ? obj._dateObj.getFullYear() : null;
    obj._month = obj._dateObj ? obj._dateObj.getMonth() + 1 : null;
    obj._absHours = faParseHoursToDecimal(obj.absenteismo);
    obj._naoCobertoHours = faParseHoursToDecimal(obj.tempoNaoCoberto);

    return obj;
  });
}

async function faLoadWithCache() {
  const now = Date.now();

  if (faltasAltumCache && now - faltasAltumCacheTime < FALTAS_ALTUM_CACHE_TTL) {
    return faltasAltumCache;
  }

  const rows = await faLoadRaw();
  faltasAltumCache = rows;
  faltasAltumCacheTime = now;

  console.log("FALTAS ALTUM cache atualizado:", rows.length);

  return faltasAltumCache;
}

function faApplyFilters(rows, query) {
  const unidade = faSplitMulti(query.unidade);
  const plantao = faSplitMulti(query.plantao);
  const turno = faSplitMulti(query.turno);
  const ocorrencia = faSplitMulti(query.ocorrencia);
  const colaborador = faSplitMulti(query.colaborador);
  const cobertura = faSplitMulti(query.cobertura);
  const ano = faSplitMulti(query.ano);
  const mes = faSplitMulti(query.mes);
  const dia = faSplitMulti(query.dia);
  const busca = faNormLower(query.busca);

  return rows.filter((row) => {
    if (!faMatchesMulti(row.unidade, unidade)) return false;
    if (!faMatchesMulti(row.plantao, plantao)) return false;
    if (!faMatchesMulti(row.turno, turno)) return false;
    if (!faMatchesMulti(row.ocorrencia, ocorrencia)) return false;
    if (!faMatchesMulti(row.colaborador, colaborador)) return false;
    if (!faMatchesMulti(row.cobertura, cobertura)) return false;
    if (ano.length && !ano.includes(String(row._year || ""))) return false;
    if (mes.length && !mes.includes(String(row._month || ""))) return false;
    if (dia.length && !dia.includes(String(row._day || ""))) return false;

    if (busca) {
      const text = Object.values(row)
        .filter((v) => typeof v !== "object")
        .join(" ")
        .toLowerCase();

      if (!text.includes(busca)) return false;
    }

    return true;
  });
}

function faApplyFiltersWithoutPeriod(rows, query) {
  return faApplyFilters(rows, {
    ...query,
    periodoTipo: "",
    dataRef: "",
  });
}

function faFilterRowsByPeriod(rows, query) {
  const period = faResolvePeriodFromQuery(query, rows);

  if (!period.start || !period.end) {
    return { rows, period };
  }

  const filtered = rows.filter((row) => {
    const dt = row._dateObj;
    return dt && dt >= period.start && dt <= period.end;
  });

  return { rows: filtered, period };
}

app.get("/api/faltas-altum-filtros", requireAuth, async (req, res) => {
  try {
    const rows = await faLoadWithCache();

    const uniq = (getter) =>
      [...new Set(rows.map(getter).map(v => faNorm(v)).filter(Boolean))]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));

    const uniqNum = (getter) =>
      [...new Set(rows.map(getter).map(v => String(v || "")).filter(Boolean))]
        .sort((a, b) => Number(a) - Number(b));

    return res.json({
      unidade: uniq(r => r.unidade),
      plantao: uniq(r => r.plantao),
      turno: uniq(r => r.turno),
      ocorrencia: uniq(r => r.ocorrencia),
      colaborador: uniq(r => r.colaborador),
      cobertura: uniq(r => r.cobertura),
      ano: uniqNum(r => r._year),
      mes: uniqNum(r => r._month),
      dia: uniqNum(r => r._day),
      ultimaAtualizacao: faGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/faltas-altum-filtros:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/faltas-altum-resumo", requireAuth, async (req, res) => {
  try {
    const rows = await faLoadWithCache();

    const filteredRows = faApplyFilters(rows, req.query);
    const { rows: periodRows, period } = faFilterRowsByPeriod(filteredRows, req.query);

    const baseFalta = periodRows.filter(faIsFaltaOuPostoVago);

    const totalOcorrencias = periodRows.length;

    // Total de pessoas que faltaram ou posto vago
    const totalQtd = baseFalta.reduce((sum, row) => {
      return sum + Number(row.qtd || 0);
    }, 0);

    // Horas descobertas / não cobertas
    const totalHorasNaoCobertas = periodRows.reduce((sum, row) => {
      return sum + Number(row._naoCobertoHours || 0);
    }, 0);

    const totalColaboradores = new Set(
      periodRows.map(r => faNorm(r.colaborador)).filter(Boolean)
    ).size;

    const totalUnidades = new Set(
      periodRows.map(r => faNorm(r.unidade)).filter(Boolean)
    ).size;

    // Base fixa de agentes considerando filtros de unidade e turno
    const baseAgentes = faGetBaseAgentesFromQuery(req.query);

    // Quantidade de dias do período filtrado
    const diasPeriodo =
      period.start && period.end
        ? faDiffDaysInclusive(period.start, period.end)
        : 1;

    // Horas planejadas corretas
    // Exemplo mês completo: 168 agentes x 12h x dias do mês
    const baseHoras = baseAgentes * 12 * diasPeriodo;

    // Horas impactadas
    // Falta/posto vago conta 12h cada + tempo realmente descoberto
    const horasFaltasPostoVago = totalQtd * 12;
    const horasImpactadas = horasFaltasPostoVago + totalHorasNaoCobertas;

    const absPercentual =
      baseHoras > 0 ? (horasImpactadas / baseHoras) * 100 : 0;

    const comparison = faGetComparisonWindow(period.start, period.end);
    const baseNoPeriodRows = faApplyFiltersWithoutPeriod(rows, req.query);

    const previousRows = baseNoPeriodRows.filter((row) => {
      const dt = row._dateObj;
      return (
        dt &&
        comparison.previousStart &&
        comparison.previousEnd &&
        dt >= comparison.previousStart &&
        dt <= comparison.previousEnd
      );
    });

    const previousBaseFalta = previousRows.filter(faIsFaltaOuPostoVago);

    const qtdAnterior = previousBaseFalta.reduce((sum, row) => {
      return sum + Number(row.qtd || 0);
    }, 0);

    const horasNaoCobertasAnterior = previousRows.reduce((sum, row) => {
      return sum + Number(row._naoCobertoHours || 0);
    }, 0);

    const diasPeriodoAnterior =
      comparison.previousStart && comparison.previousEnd
        ? faDiffDaysInclusive(comparison.previousStart, comparison.previousEnd)
        : diasPeriodo;

    const baseHorasAnterior = baseAgentes * 12 * diasPeriodoAnterior;

    const horasImpactadasAnterior =
      (qtdAnterior * 12) + horasNaoCobertasAnterior;

    const absPercentualAnterior =
      baseHorasAnterior > 0
        ? (horasImpactadasAnterior / baseHorasAnterior) * 100
        : 0;

    return res.json({
      totalOcorrencias,
      totalQtd,

      totalHorasNaoCobertas,
      totalHorasNaoCobertasFmt: faFormatHoursDecimal(totalHorasNaoCobertas),

      totalColaboradores,
      totalUnidades,

      baseAgentes,
      diasPeriodo,
      baseHoras,

      horasFaltasPostoVago,
      horasFaltasPostoVagoFmt: faFormatHoursDecimal(horasFaltasPostoVago),

      horasImpactadas,
      horasImpactadasFmt: faFormatHoursDecimal(horasImpactadas),

      absPercentual,
      absPercentualAnterior,

      variacaoQtd: faPercentChange(totalQtd, qtdAnterior),
      variacaoAbs: faPercentChange(absPercentual, absPercentualAnterior),

      comparativoLabel: comparison.label || "base anterior",
      periodoAtualInicio: comparison.currentStart ? faFormatBR(comparison.currentStart) : "",
      periodoAtualFim: comparison.currentEnd ? faFormatBR(comparison.currentEnd) : "",
      periodoAnteriorInicio: comparison.previousStart ? faFormatBR(comparison.previousStart) : "",
      periodoAnteriorFim: comparison.previousEnd ? faFormatBR(comparison.previousEnd) : "",

      ultimaAtualizacao: faGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/faltas-altum-resumo:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/faltas-altum-graficos", requireAuth, async (req, res) => {
  try {
    const rows = await faLoadWithCache();

    const filteredRows = faApplyFilters(rows, req.query);
    const { rows: periodRows, period } = faFilterRowsByPeriod(filteredRows, req.query);

    const baseFalta = periodRows.filter(faIsFaltaOuPostoVago);

    const porDia = {};
    for (let i = 1; i <= 31; i++) porDia[i] = 0;

    const porOcorrencia = {};
    const porUnidade = {};
    const porTurno = {};
    const porPlantao = {};

    baseFalta.forEach((row) => {
      const qtd = Number(row.qtd || 0);

      if (row._day) porDia[row._day] += qtd;

      const ocorrencia = faNorm(row.ocorrencia) || "Sem ocorrência";
      porOcorrencia[ocorrencia] = (porOcorrencia[ocorrencia] || 0) + qtd;

      const unidade = faNorm(row.unidade) || "Sem unidade";
      porUnidade[unidade] = (porUnidade[unidade] || 0) + qtd;

      const turno = faNorm(row.turno) || "Sem turno";
      porTurno[turno] = (porTurno[turno] || 0) + qtd;

      const plantao = faNorm(row.plantao) || "Sem plantão";
      porPlantao[plantao] = (porPlantao[plantao] || 0) + qtd;
    });

    const comparison = faGetComparisonWindow(period.start, period.end);
    const baseNoPeriodRows = faApplyFiltersWithoutPeriod(rows, req.query);
    const previousBaseFalta = baseNoPeriodRows
      .filter((row) => {
        const dt = row._dateObj;
        return (
          dt &&
          comparison.previousStart &&
          comparison.previousEnd &&
          dt >= comparison.previousStart &&
          dt <= comparison.previousEnd
        );
      })
      .filter(faIsFaltaOuPostoVago);

    const atual = baseFalta.reduce((sum, row) => sum + Number(row.qtd || 0), 0);
    const anterior = previousBaseFalta.reduce((sum, row) => sum + Number(row.qtd || 0), 0);

    return res.json({
      porDia,
      porOcorrencia,
      porUnidade,
      porTurno,
      porPlantao,
      comparativo: {
        atual,
        anterior,
        label: comparison.label || "base anterior",
      },
      ultimaAtualizacao: faGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/faltas-altum-graficos:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/faltas-altum-detalhes", requireAuth, async (req, res) => {
  try {
    const rows = await faLoadWithCache();

    const filteredRows = faApplyFilters(rows, req.query);
    const { rows: periodRows } = faFilterRowsByPeriod(filteredRows, req.query);

    const page = Math.max(Number(req.query.page || 1), 1);
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 200);

    const sorted = [...periodRows].sort((a, b) => {
      const ad = a._dateObj ? a._dateObj.getTime() : 0;
      const bd = b._dateObj ? b._dateObj.getTime() : 0;
      return bd - ad;
    });

    const total = sorted.length;
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const currentPage = Math.min(page, totalPages);
    const start = (currentPage - 1) * limit;

    const rowsOut = sorted.slice(start, start + limit).map((row) => ({
      data: row._dateObj ? faFormatBR(row._dateObj) : row.data || "",
      unidade: row.unidade || "",
      plantao: row.plantao || "",
      turno: row.turno || "",
      ocorrencia: row.ocorrencia || "",
      qtd: Number(row.qtd || 0),
      absenteismo: row.absenteismo || "",
      colaborador: row.colaborador || "",
      cobertura: row.cobertura || "",
    }));

    return res.json({
      total,
      page: currentPage,
      limit,
      totalPages,
      rows: rowsOut,
      ultimaAtualizacao: faGetHoraAtualizacaoBR(),
    });
  } catch (error) {
    console.error("Erro /api/faltas-altum-detalhes:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== APP MARCACAO DIARIA ==================

app.get("/marcacao", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "marcacao.html"));
});

const MARCACAO_SPREADSHEET_ID = "1DivjZdxzDES6Nu_ZJI_1gmrJgKqlrlqYgg3I-ybPr5k";
const MARCACAO_BASE_RANGE = "'BASE DE AGENTES'!A1:Z200000";
const MARCACAO_REGISTRO_RANGE = "'REGISTRO DE PRESENTEÍSMO'!A1:Z200000";

// ================== HELPERS ==================

function mcNorm(v) {
  return String(v || "").trim();
}

function mcNormLower(v) {
  return mcNorm(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function mcFormatDateBR(iso) {
  if (!iso) return "";
  const [y, m, d] = String(iso).split("-");
  if (!y || !m || !d) return "";
  return `${d}/${m}/${y}`;
}

function mcMesNome(dateObj) {
  if (!(dateObj instanceof Date) || isNaN(dateObj.getTime())) return "";
  return dateObj.toLocaleDateString("pt-BR", { month: "long" }).toUpperCase();
}

function mcFindHeader(headers, possibilities) {
  const normalized = headers.map(h => mcNormLower(h));
  for (const p of possibilities) {
    const idx = normalized.findIndex(h => h === mcNormLower(p));
    if (idx !== -1) return headers[idx];
  }
  return null;
}

function mcStatusMap(codigo) {
  const map = {
    P: {
      status: "PRESENTE",
      cobertura: "",
      abs: "00:00:00",
      cobrindo: "",
      horaCobertura: "",
    },
    F: {
      status: "FALTA",
      cobertura: "Sem cobertura",
      abs: "12:00:00",
      cobrindo: "",
      horaCobertura: "",
    },
    FE: {
      status: "FÉRIAS",
      cobertura: "",
      abs: "12:00:00",
      cobrindo: "",
      horaCobertura: "",
    },
    FC: {
      status: "FALTA C/ COBERTURA",
      cobertura: "Cobertura parcial",
      abs: "12:00:00",
      cobrindo: "",
      horaCobertura: "",
    },
    PV: {
      status: "POSTO VAGO",
      cobertura: "",
      abs: "12:00:00",
      cobrindo: "",
      horaCobertura: "",
    },
  };

  return map[codigo] || {
    status: "",
    cobertura: "",
    abs: "00:00:00",
    cobrindo: "",
    horaCobertura: "",
  };
}

// ================== LOAD BASE DE AGENTES ==================

async function mcLoadBaseAgentes() {
  const sheets = await conectarSheetsEdicao();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: MARCACAO_SPREADSHEET_ID,
    range: MARCACAO_BASE_RANGE,
  });

  const values = res.data.values || [];
  if (!values.length) {
    return { headers: [], rows: [] };
  }

  const headers = values[0].map((h, i) => mcNorm(h) || `COL_${i + 1}`);

  const rows = values.slice(1).map((row) => {
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = row[i] || "";
    });
    return obj;
  });

  return { headers, rows };
}

// ================== LOAD REGISTRO ==================

async function mcLoadRegistro() {
  const sheets = await conectarSheetsEdicao();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: MARCACAO_SPREADSHEET_ID,
    range: MARCACAO_REGISTRO_RANGE,
  });

  const values = res.data.values || [];
  if (!values.length) {
    return { headers: [], rows: [] };
  }

  const headers = values[0].map((h, i) => mcNorm(h) || `COL_${i + 1}`);

  const rows = values.slice(1).map((row, idx) => {
    const obj = { _sheetRow: idx + 2 };
    headers.forEach((h, i) => {
      obj[h] = row[i] || "";
    });
    return obj;
  });

  return { headers, rows };
}

// ================== FILTROS ==================

app.get("/api/marcacao-unidades", requireAuth, async (req, res) => {
  try {
    const base = await mcLoadBaseAgentes();
    const headers = base.headers;
    const rows = base.rows;

    const hUnidade = mcFindHeader(headers, ["UNIDADE", "UNIDADE OPERACIONAL"]);

    if (!hUnidade) {
      return res.status(400).json({
        ok: false,
        message: "Coluna UNIDADE não encontrada na BASE DE AGENTES."
      });
    }

    const unidades = [...new Set(
      rows.map(r => mcNorm(r[hUnidade])).filter(Boolean)
    )].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json({ ok: true, unidades });
  } catch (error) {
    console.error("Erro /api/marcacao-unidades:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/marcacao-lideres", requireAuth, async (req, res) => {
  try {
    const unidade = mcNorm(req.query.unidade);

    if (!unidade) {
      return res.json({ ok: true, lideres: [] });
    }

    const base = await mcLoadBaseAgentes();
    const headers = base.headers;
    const rows = base.rows;

    const hUnidade = mcFindHeader(headers, ["UNIDADE", "UNIDADE OPERACIONAL"]);
    const hPlantao = mcFindHeader(headers, ["PLANTÃO", "PLANTAO", "LÍDER", "LIDER"]);

    if (!hUnidade || !hPlantao) {
      return res.status(400).json({
        ok: false,
        message: "Colunas UNIDADE/PLANTÃO não encontradas na BASE DE AGENTES."
      });
    }

    const lideres = [...new Set(
      rows
        .filter(r => mcNorm(r[hUnidade]) === unidade)
        .map(r => mcNorm(r[hPlantao]))
        .filter(Boolean)
    )].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return res.json({ ok: true, lideres });
  } catch (error) {
    console.error("Erro /api/marcacao-lideres:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== CARREGAR EFETIVO ==================

app.get("/api/marcacao-efetivo", requireAuth, async (req, res) => {
  try {
    const unidade = mcNorm(req.query.unidade);
    const lider = mcNorm(req.query.lider);
    const dataISO = mcNorm(req.query.data);

    if (!unidade || !lider || !dataISO) {
      return res.status(400).json({
        ok: false,
        message: "Informe unidade, líder e data."
      });
    }

    const base = await mcLoadBaseAgentes();
    const registro = await mcLoadRegistro();

    const bh = base.headers;
    const br = base.rows;
    const rh = registro.headers;
    const rr = registro.rows;

    const hBaseUnidade = mcFindHeader(bh, ["UNIDADE", "UNIDADE OPERACIONAL"]);
    const hBasePlantao = mcFindHeader(bh, ["PLANTÃO", "PLANTAO", "LÍDER", "LIDER"]);
    const hBaseAgente = mcFindHeader(bh, ["AGENTE", "COLABORADOR", "NOME"]);
    const hBaseRE = mcFindHeader(bh, ["RE", "MATRÍCULA", "MATRICULA"]);
    const hBaseCargo = mcFindHeader(bh, ["CARGO", "FUNÇÃO", "FUNCAO"]);
    const hBaseTurno = mcFindHeader(bh, ["TURNO", "ESCALA"]);
    const hBaseEmpresa = mcFindHeader(bh, ["EMPRESA"]);
    const hBaseStatus = mcFindHeader(bh, ["STATUS"]);

    if (!hBaseUnidade || !hBasePlantao || !hBaseAgente) {
      return res.status(400).json({
        ok: false,
        message: "Colunas mínimas não encontradas na BASE DE AGENTES."
      });
    }

    let efetivo = br
      .filter(r =>
        mcNorm(r[hBaseUnidade]) === unidade &&
        mcNorm(r[hBasePlantao]) === lider
      )
      .map(r => ({
        UNIDADE: mcNorm(r[hBaseUnidade]),
        EMPRESA: mcNorm(hBaseEmpresa ? r[hBaseEmpresa] : ""),
        "PLANTÃO": mcNorm(r[hBasePlantao]),
        TURNO: mcNorm(hBaseTurno ? r[hBaseTurno] : ""),
        AGENTE: mcNorm(r[hBaseAgente]),
        RE: mcNorm(hBaseRE ? r[hBaseRE] : ""),
        CARGO: mcNorm(hBaseCargo ? r[hBaseCargo] : ""),
        STATUS_BASE: mcNorm(hBaseStatus ? r[hBaseStatus] : ""),
      }));

    const existeLider = efetivo.some(r => mcNormLower(r.AGENTE) === mcNormLower(lider));
    if (!existeLider) {
      const ref = efetivo[0] || {};
      efetivo.unshift({
        UNIDADE: unidade,
        EMPRESA: ref.EMPRESA || "",
        "PLANTÃO": lider,
        TURNO: ref.TURNO || "",
        AGENTE: lider,
        RE: "",
        CARGO: "LÍDER",
        STATUS_BASE: "ATIVO",
      });
    }

    const hRegData = mcFindHeader(rh, ["DATA"]);
    const hRegUnidade = mcFindHeader(rh, ["UNIDADE"]);
    const hRegPlantao = mcFindHeader(rh, ["PLANTÃO", "PLANTAO"]);
    const hRegAgente = mcFindHeader(rh, ["AGENTE"]);
    const hRegStatus = mcFindHeader(rh, ["STATUS"]);
    const hRegCobertura = mcFindHeader(rh, ["STATUS DE COBERTURA"]);
    const hRegAbs = mcFindHeader(rh, ["ABSENTEISMO", "ABSENTEÍSMO"]);
    const hRegCobrindo = mcFindHeader(rh, ["COLABORADOR COBRINDO POSTO"]);
    const hRegHoraCobertura = mcFindHeader(rh, ["HORA COBERTURA"]);

    const dataBR = mcFormatDateBR(dataISO);

    const rows = efetivo.map(item => {
      const jaLancado = rr.find(r =>
        mcNorm(hRegData ? r[hRegData] : "") === dataBR &&
        mcNorm(hRegUnidade ? r[hRegUnidade] : "") === unidade &&
        mcNorm(hRegPlantao ? r[hRegPlantao] : "") === lider &&
        mcNormLower(hRegAgente ? r[hRegAgente] : "") === mcNormLower(item.AGENTE)
      );

      let marcacao = "";

      if (jaLancado) {
        const st = mcNormLower(hRegStatus ? jaLancado[hRegStatus] : "");
        if (st === "presente") marcacao = "P";
        else if (st === "férias" || st === "ferias") marcacao = "FE";
        else if (st.includes("c/ cobertura")) marcacao = "FC";
        else if (st === "posto vago") marcacao = "PV";
        else if (st === "falta") marcacao = "F";
      }

      return {
        ...item,
        marcacao,
        status: jaLancado && hRegStatus ? jaLancado[hRegStatus] : "",
        statusCobertura: jaLancado && hRegCobertura ? jaLancado[hRegCobertura] : "",
        cobrindo: jaLancado && hRegCobrindo ? jaLancado[hRegCobrindo] : "",
        horaCobertura: jaLancado && hRegHoraCobertura ? jaLancado[hRegHoraCobertura] : "",
        abs: jaLancado && hRegAbs ? jaLancado[hRegAbs] : "00:00:00",
      };
    });

    return res.json({ ok: true, rows });
  } catch (error) {
    console.error("Erro /api/marcacao-efetivo:", error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

// ================== SALVAR MARCACAO ==================

app.post("/api/marcacao-salvar", requireAuth, async (req, res) => {
  try {
    const { unidade, lider, data, rows } = req.body || {};

    if (!unidade || !lider || !data || !Array.isArray(rows)) {
      return res.status(400).json({
        ok: false,
        message: "Payload inválido."
      });
    }

    const sheets = await conectarSheetsEdicao();
    const registro = await mcLoadRegistro();

    let headers = registro.headers;
    if (!headers.length) {
      headers = [
        "MÊS",
        "DATA",
        "UNIDADE",
        "EMPRESA",
        "PLANTÃO",
        "TURNO",
        "AGENTE",
        "STATUS",
        "STATUS DE COBERTURA",
        "ABSENTEISMO",
        "COLABORADOR COBRINDO POSTO",
        "HORA COBERTURA"
      ];

      await sheets.spreadsheets.values.update({
        spreadsheetId: MARCACAO_SPREADSHEET_ID,
        range: `'REGISTRO DE PRESENTEÍSMO'!A1:L1`,
        valueInputOption: "USER_ENTERED",
        requestBody: { values: [headers] }
      });

      registro.headers = headers;
      registro.rows = [];
    }

    const hRegData = mcFindHeader(headers, ["DATA"]);
    const hRegUnidade = mcFindHeader(headers, ["UNIDADE"]);
    const hRegPlantao = mcFindHeader(headers, ["PLANTÃO", "PLANTAO"]);
    const hRegAgente = mcFindHeader(headers, ["AGENTE"]);

    const dataObj = new Date(`${data}T00:00:00`);
    if (isNaN(dataObj.getTime())) {
      return res.status(400).json({
        ok: false,
        message: "Data inválida."
      });
    }

    const dataBR = mcFormatDateBR(data);
    const mesNome = mcMesNome(dataObj);

    const updates = [];
    const appends = [];

    for (const row of rows) {
      const codigo = mcNorm(row.marcacao);
      if (!codigo) continue;

      const meta = mcStatusMap(codigo);

      const payloadRow = [
        mesNome,
        dataBR,
        mcNorm(row.UNIDADE || unidade),
        mcNorm(row.EMPRESA || ""),
        mcNorm(row["PLANTÃO"] || lider),
        mcNorm(row.TURNO || ""),
        mcNorm(row.AGENTE || ""),
        mcNorm(meta.status),
        mcNorm(codigo === "FC" ? "Cobertura parcial" : meta.cobertura),
        mcNorm(codigo === "FC" ? (row.abs || meta.abs) : (row.abs || meta.abs)),
        mcNorm(codigo === "FC" ? (row.cobrindo || "") : ""),
        mcNorm(codigo === "FC" ? (row.horaCobertura || "") : "")
      ];

      const existente = registro.rows.find(r =>
        mcNorm(hRegData ? r[hRegData] : "") === dataBR &&
        mcNorm(hRegUnidade ? r[hRegUnidade] : "") === mcNorm(row.UNIDADE || unidade) &&
        mcNorm(hRegPlantao ? r[hRegPlantao] : "") === mcNorm(row["PLANTÃO"] || lider) &&
        mcNormLower(hRegAgente ? r[hRegAgente] : "") === mcNormLower(row.AGENTE || "")
      );

      if (existente) {
        updates.push({
          rowNumber: existente._sheetRow,
          values: payloadRow
        });
      } else {
        appends.push(payloadRow);
      }
    }

    for (const upd of updates) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: MARCACAO_SPREADSHEET_ID,
        range: `'REGISTRO DE PRESENTEÍSMO'!A${upd.rowNumber}:L${upd.rowNumber}`,
        valueInputOption: "USER_ENTERED",
        requestBody: {
          values: [upd.values]
        }
      });
    }

    if (appends.length) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: MARCACAO_SPREADSHEET_ID,
        range: `'REGISTRO DE PRESENTEÍSMO'!A:L`,
        valueInputOption: "USER_ENTERED",
        insertDataOption: "INSERT_ROWS",
        requestBody: {
          values: appends
        }
      });
    }

    return res.json({
      ok: true,
      message: `Salvo com sucesso. Atualizados: ${updates.length} | Novos: ${appends.length}`
    });
  } catch (error) {
    console.error("Erro /api/marcacao-salvar:", error?.response?.data || error.message || error);
    return res.status(500).json({
      ok: false,
      message: error?.response?.data?.error?.message || error.message || "Erro ao salvar marcação."
    });
  }
});

// ================== AV- LOST ==================

function normalizeText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeName(value) {
  if (!value) return "";

  const name = normalizeText(value)
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  const corrections = {
    ERICK: "ERIK",
    ERIC: "ERIK",
    BIANKA: "BIANCA",
    "BIANCA ": "BIANCA",
    "HEITOR ": "HEITOR",
  };

  return corrections[name] || name;
}

function normalizeStatus(value) {
  return normalizeText(value).toUpperCase();
}

function normalizeUnidade(value) {
  return normalizeText(value).toUpperCase();
}

async function buscarPlanilha() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "1NTliBuXKzIXE99Lj3O2oT1B5PovdL7mTaTCReM-LbvM",
    range: "AV!A:M",
  });

  return response.data.values || [];
}

app.get("/api/dados", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilha();

    if (!dados || dados.length === 0) {
      return res.json([]);
    }

    const cabecalho = dados[0] || [];
    const linhas = dados.slice(1);

    const objetos = linhas.map((linha) => {
      const obj = {};

      cabecalho.forEach((col, i) => {
        const columnName = normalizeText(col);
        let value = linha[i] ?? "";

        const colUpper = columnName.toUpperCase();

        if (colUpper.includes("NOME") || colUpper.includes("COLABORADOR")) {
          value = normalizeName(value);
        }

        if (colUpper.includes("STATUS")) {
          value = normalizeStatus(value);
        }

        if (colUpper.includes("UNIDADE")) {
          value = normalizeUnidade(value);
        }

        obj[columnName] = normalizeText(value);
      });

      return obj;
    });

    return res.json(objetos);
  } catch (e) {
    console.log("Erro /api/dados:", e);
    return res.json([]);
  }
});


// ================== VARREDURA ==================

function normalizeTextVarredura(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeUpperVarredura(value) {
  return normalizeTextVarredura(value)
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

async function buscarPlanilhaVarredura() {
  const sheets = await conectarSheets();

  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: "1NTliBuXKzIXE99Lj3O2oT1B5PovdL7mTaTCReM-LbvM",
    range: "VARREDURA!A:O",
  });

  return response.data.values || [];
}

app.get("/api/varredura", requireAuth, async (req, res) => {
  try {
    const dados = await buscarPlanilhaVarredura();

    if (!dados || dados.length === 0) {
      return res.json([]);
    }

    const cabecalho = dados[0] || [];
    const linhas = dados.slice(1);

    const objetos = linhas.map((linha) => {
      const obj = {};

      cabecalho.forEach((col, i) => {
        const columnName = normalizeTextVarredura(col);
        let value = linha[i] ?? "";

        const colUpper = columnName.toUpperCase();

        if (
          colUpper.includes("NOME") ||
          colUpper.includes("UNIDADE") ||
          colUpper.includes("TURNO") ||
          colUpper.includes("PROCESSO") ||
          colUpper.includes("STATUS") ||
          colUpper.includes("VALIDAÇÃO") ||
          colUpper.includes("VALIDACAO")
        ) {
          value = normalizeUpperVarredura(value);
        }

        obj[columnName] = normalizeTextVarredura(value);
      });

      return obj;
    });

    return res.json(objetos);
  } catch (erro) {
    console.error("Erro /api/varredura:", erro);
    return res.json([]);
  }
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



// ================== ERROS ==================

app.use((req, res) => {
  return res.status(404).send("Página não encontrada.");
});

app.use((err, req, res, next) => {
  console.error("Erro não tratado:", err);
  return res.status(500).send("Erro interno do servidor.");
});


// ================== START ==================

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
  console.log("Google OAuth:", googleOAuthEnabled ? "habilitado" : "desabilitado");
  console.log("Callback Google:", GOOGLE_CALLBACK_URL);
  console.log("Cadastro Sheet ID:", CADASTRO_SHEET_ID ? "configurado" : "não configurado");
});
