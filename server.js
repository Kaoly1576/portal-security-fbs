require("dotenv").config();

const express = require("express");
const session = require("express-session");
const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;
const path = require("path");

const app = express();
const PORT = Number(process.env.PORT || 8080);

// ================== CONSTANTES ==================

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_CALLBACK_URL =
  process.env.GOOGLE_CALLBACK_URL ||
  `http://localhost:3000/auth/google/callback`;

const googleOAuthEnabled =
  Boolean(GOOGLE_CLIENT_ID) && Boolean(GOOGLE_CLIENT_SECRET);

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
      secure: process.env.NODE_ENV === "production",
      maxAge: 1000 * 60 * 60 * 12,
    },
  })
);

app.use(passport.initialize());
app.use(express.static(path.join(__dirname, "public")));

// ================== HELPERS ==================

function requireAuth(req, res, next) {
  if (!req.session.userId) {
    return res.redirect("/login");
  }
  next();
}

function requireAprovador(req, res, next) {
  if (!req.session.userId) {
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
    nivel === "admin";

  if (!podeAprovar) {
    return res.status(403).send("Acesso negado.");
  }

  next();
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

// ================== GOOGLE OAUTH ==================

if (googleOAuthEnabled) {
  passport.use(
    new GoogleStrategy(
      {
        clientID: GOOGLE_CLIENT_ID,
        clientSecret: GOOGLE_CLIENT_SECRET,
        callbackURL: GOOGLE_CALLBACK_URL,
      },
      async (accessToken, refreshToken, profile, done) => {
        try {
          const email = profile.emails?.[0]?.value || "";
          const foto = profile.photos?.[0]?.value || "";

          return done(null, {
            id: profile.id,
            nome: profile.displayName || "",
            email,
            foto,
            perfil: "usuario",
            aprovador: "0",
            nivel_acesso: "1",
          });
        } catch (error) {
          return done(error, null);
        }
      }
    )
  );

  console.log("Google OAuth habilitado.");
} else {
  console.warn(
    "Google OAuth desabilitado: faltam GOOGLE_CLIENT_ID ou GOOGLE_CLIENT_SECRET."
  );
}

// ================== ROTAS ==================

app.get("/login", (req, res) => {
  if (!googleOAuthEnabled) {
    return res.status(500).send(`
      <h1>Login indisponível</h1>
      <p>As variáveis do Google OAuth não foram configuradas.</p>
    `);
  }

  return res.send(`
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Login</title>
      </head>
      <body style="font-family: Arial; padding: 40px;">
        <h1>Portal Security</h1>
        <p>Faça login com sua conta Google.</p>
        <a href="/auth/google" style="display:inline-block;padding:12px 18px;background:#111;color:#fff;text-decoration:none;border-radius:8px;">
          Entrar com Google
        </a>
      </body>
    </html>
  `);
});

app.get(
  "/auth/google",
  (req, res, next) => {
    if (!googleOAuthEnabled) {
      return res.status(500).send("Google OAuth não configurado.");
    }
    next();
  },
  passport.authenticate("google", {
    scope: ["profile", "email"],
    session: false,
  })
);

app.get("/auth/google/callback", (req, res, next) => {
  if (!googleOAuthEnabled) {
    return res.status(500).send("Google OAuth não configurado.");
  }

  passport.authenticate(
    "google",
    {
      failureRedirect: "/login",
      session: false,
    },
    (err, user) => {
      if (err) {
        console.error("Erro no callback Google:", err);
        return res.status(500).send("Erro no login com Google.");
      }

      if (!user) {
        return res.redirect("/login");
      }

      req.session.userId = user.id;
      req.session.nome = user.nome;
      req.session.email = user.email;
      req.session.foto = user.foto;
      req.session.perfil = user.perfil;
      req.session.aprovador = user.aprovador;
      req.session.nivel_acesso = user.nivel_acesso;

      req.session.save((saveErr) => {
        if (saveErr) {
          console.error("Erro ao salvar sessão:", saveErr);
          return res.status(500).send("Erro ao salvar sessão.");
        }

        return res.redirect("/");
      });
    }
  )(req, res, next);
});

app.get("/", requireAuth, (req, res) => {
  res.send(`
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Painel</title>
      </head>
      <body style="font-family: Arial; padding: 40px;">
        <h1>Usuário autenticado</h1>
        <p><strong>Nome:</strong> ${req.session.nome || "-"}</p>
        <p><strong>Email:</strong> ${req.session.email || "-"}</p>
        <p><strong>Perfil:</strong> ${req.session.perfil || "-"}</p>
        <p><strong>Nível:</strong> ${req.session.nivel_acesso || "-"}</p>
        <p><a href="/aprovacoes">Área de aprovações</a></p>
        <p><a href="/logout">Sair</a></p>
      </body>
    </html>
  `);
});

app.get("/aprovacoes", requireAprovador, (req, res) => {
  res.send(`
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Aprovações</title>
      </head>
      <body style="font-family: Arial; padding: 40px;">
        <h1>Área de aprovações</h1>
        <p>Você tem permissão de aprovador.</p>
        <p><a href="/">Voltar</a></p>
      </body>
    </html>
  `);
});

app.get("/logout", (req, res) => {
  req.session.destroy(() => {
    res.clearCookie("portal_security_sid");
    res.redirect("/login");
  });
});

// ================== ERROS ==================

app.use((err, req, res, next) => {
  console.error("Erro não tratado:", err);
  res.status(500).send("Erro interno do servidor.");
});

// ================== START ==================

app.listen(PORT, () => {
  console.log(`Servidor rodando: http://localhost:${PORT}`);
});

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

// ================== LOGOUT =================

app.get("/logout", (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error("Erro ao destruir sessão:", err);
      return res.redirect("/portal");
    }

    res.clearCookie("connect.sid");
    return res.redirect("/login");
  });
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

// ================== LOGOUT =================

app.get("/logout", (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error("Erro ao destruir sessão:", err);
      return res.redirect("/portal");
    }

    res.clearCookie("connect.sid");
    return res.redirect("/login");
  });
});

