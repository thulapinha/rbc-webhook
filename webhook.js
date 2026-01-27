// File: webhook.js
// Webhook Mercado Pago — Render (ESM)
// ✅ Compatível com "type": "module"
// ✅ Idempotente
// ✅ PIX / Cartão / Boleto
// ✅ Parse Server

import express from "express";
import axios from "axios";
import Parse from "parse/node";

const app = express();
app.use(express.json());

// =====================================================
// 🔐 CONFIG — TOKENS DIRETO NO CÓDIGO (TESTE)
// =====================================================
const MP_MODE = "test"; // "test" | "prod"

const MP_KEYS = {
  test: {
    accessToken: "TEST-APP_USR-712868030410210-012422-c7031be0b237288c2ffe5c809e99e5f7",
  },
  prod: {
    accessToken: "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315",
  },
};

const PARSE_APP_ID = "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz";
const PARSE_JS_KEY = "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw";
const PARSE_MASTER_KEY = "Ou385YEpEfoT3gZ6hLSbTfKZYQtTgNA7WNBnv7ia";
const PARSE_SERVER_URL = "https://parseapi.back4app.com";

// =====================================================
// Init Parse
// =====================================================
Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY, PARSE_MASTER_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// =====================================================
function mpAccessToken() {
  return MP_KEYS[MP_MODE].accessToken;
}

function toInt(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

async function mpGetPayment(paymentId) {
  const { data } = await axios.get(
    `https://api.mercadopago.com/v1/payments/${paymentId}`,
    {
      headers: {
        Authorization: `Bearer ${mpAccessToken()}`,
      },
    }
  );
  return data;
}

// =====================================================
// Routes
// =====================================================
app.get("/pagamento", (_, res) => {
  res.status(200).send("Webhook OK");
});

app.post("/pagamento", async (req, res) => {
  try {
    const paymentId = toInt(req.body?.data?.id || req.body?.resource);
    if (!paymentId) return res.sendStatus(400);

    const mp = await mpGetPayment(paymentId);

    const status = mp.status;
    const valor = mp.transaction_amount || 0;
    const userId = mp.metadata?.user_id;

    if (status !== "approved" || !userId) {
      return res.sendStatus(200);
    }

    const q = new Parse.Query(Parse.User);
    const user = await q.get(userId, { useMasterKey: true });

    const saldo = user.get("saldo") || 0;
    user.set("saldo", saldo + valor);
    await user.save(null, { useMasterKey: true });

    console.log("✅ Crédito aplicado:", userId, valor);
    res.sendStatus(200);
  } catch (err) {
    console.error("❌ Webhook erro:", err?.message);
    res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () =>
  console.log(`🚀 Webhook rodando na porta ${PORT} [${MP_MODE}]`)
);
