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

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_CALLBACK_URL =
  process.env.GOOGLE_CALLBACK_URL ||
  "https://portal-security-fbs-production.up.railway.app/auth/google/callback";

const SESSION_SECRET = process.env.SESSION_SECRET || "troque_essa_chave";

const CADASTRO_SHEET_ID = process.env.CADASTRO_SHEET_ID || "";
const CADASTRO_USUARIOS_RANGE =
  process.env.CADASTRO_USUARIOS_RANGE || "usuarios!A:R";
const CADASTRO_CARGOS_RANGE =
  process.env.CADASTRO_CARGOS_RANGE || "cargos!A:Z";
const CADASTRO_NIVEIS_RANGE =
  process.env.CADASTRO_NIVEIS_RANGE || "niveis_acesso!A:Z";

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
  if (req.session?.userId) return res.redirect("/porta.html");
  return res.sendFile(path.join(__dirname, "public", "login.html"));
});

app.get("/login", (req, res) => {
  if (req.session?.userId) return res.redirect("/porta.html");
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
      return res.redirect("/porta.html");
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

app.get("/porta.html", requireAuth, (req, res) => {
  return res.sendFile(path.join(__dirname, "public", "porta.html"));
});

app.get("/portal", requireAuth, (req, res) => {
  return res.redirect("/porta.html");
});

app.get("/portal.html", requireAuth, (req, res) => {
  return res.redirect("/porta.html");
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
      return res.redirect("/porta.html");
    }

    res.clearCookie("portal_security_sid");
    return res.redirect("/login");
  });
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
