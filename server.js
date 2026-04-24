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
  `http://localhost:${PORT}/auth/google/callback`;

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

