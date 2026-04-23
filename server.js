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

