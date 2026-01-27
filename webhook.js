// File: webhook.js
// Webhook Mercado Pago — Render (ESM / Node 22)
// ✅ Não quebra por "type": "module"
// ✅ Busca payment no MP
// ✅ Atualiza PaymentsV2 no Parse
// ✅ Crédito idempotente (não credita 2x)

// IMPORTANTÃO:
// - Em ESM no Node 22, Parse precisa ser importado como "parse/node.js"

import express from "express";
import axios from "axios";
import Parse from "parse/node.js";

const app = express();
app.use(express.json());

// =====================================================
// 🔐 CONFIG (use ENV no Render pra não vazar segredo)
// =====================================================
const MP_MODE = (process.env.MP_MODE || "test").trim().toLowerCase(); // test|prod

const MP_KEYS = {
  test: {
    accessToken: (process.env.MP_ACCESS_TOKEN_TEST || "APP_USR-712868030410210-012422-c7031be0b237288c2ffe5c809e99e5f7-2510340016").trim(),
  },
  prod: {
    accessToken: (process.env.MP_ACCESS_TOKEN || "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315").trim(),
  },
};

function mpAccessToken() {
  const cfg = MP_KEYS[MP_MODE === "prod" ? "prod" : "test"];
  return (cfg.accessToken || "").trim();
}
// ✅ Credenciais Parse (ENV > fallback hardcoded)
const PARSE_APP_ID = (process.env.PARSE_APP_ID || "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz").trim();
const PARSE_JS_KEY = (process.env.PARSE_JS_KEY || "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw").trim();
const PARSE_MASTER_KEY = (process.env.PARSE_MASTER_KEY || "Ou385YEpEfoT3gZ6hLSbTfKZYQtTgNA7WNBnv7ia").trim();
const PARSE_SERVER_URL = (process.env.PARSE_SERVER_URL || "https://parseapi.back4app.com").trim();

if (!PARSE_APP_ID || !PARSE_JS_KEY || !PARSE_MASTER_KEY) {
  console.warn("⚠️ Parse keys faltando. Configure no Render > Environment.");
}

// Init Parse
Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY, PARSE_MASTER_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// =====================================================
// Helpers
// =====================================================
function toInt(x) {
  const n = parseInt(x, 10);
  return Number.isFinite(n) ? n : null;
}

function parseUserIdFromExternalRef(externalRef) {
  const s = (externalRef || "").toString().trim();
  // formato novo: rbc:<userId>:<payId>:<metodo>
  if (s.startsWith("rbc:")) {
    const parts = s.split(":");
    if (parts.length >= 2 && parts[1]) return parts[1];
  }
  // formato legado: external_reference = userId
  if (s && s.length >= 6) return s;
  return null;
}

async function mpGetPayment(paymentId) {
  const token = mpAccessToken();
  if (!token) throw new Error("MP access token não configurado (ENV).");

  const { data } = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: 15000,
  });

  return data;
}

async function creditSaldoIdempotente({ userId, valor, referencia }) {
  const userQuery = new Parse.Query(Parse.User);
  const user = await userQuery.get(userId, { useMasterKey: true });
  if (!user) throw new Error("Usuário não encontrado para crédito.");

  let saldoAtual = user.get("saldo");
  if (typeof saldoAtual !== "number") saldoAtual = 0;

  let historico = user.get("historico") || [];
  if (!Array.isArray(historico)) historico = [];

  const refStr = `ref:${referencia}`;
  if (historico.includes(refStr)) {
    return { duplicated: true, novoSaldo: saldoAtual };
  }

  const novoSaldo = saldoAtual + valor;
  user.set("saldo", novoSaldo);
  historico.push(refStr);
  user.set("historico", historico);
  await user.save(null, { useMasterKey: true });

  const hist = new Parse.Object("SaldoHistorico");
  hist.set("nome", user.get("name") || "—");
  hist.set("email", user.get("email") || "—");
  hist.set("valor", valor);
  hist.set("tipo", "deposito");
  hist.set("descricao", `Crédito confirmado (${referencia})`);
  await hist.save(null, { useMasterKey: true });

  return { duplicated: false, novoSaldo };
}

async function upsertPaymentV2({ mpPaymentId, status, statusDetail, userId, valor, metodo, externalReference }) {
  const q = new Parse.Query("PaymentsV2");
  q.equalTo("mpPaymentId", mpPaymentId);
  let local = await q.first({ useMasterKey: true });

  if (!local) {
    local = new Parse.Object("PaymentsV2");
    local.set("mpPaymentId", mpPaymentId);
    local.set("status", status || "unknown");
    local.set("statusDetail", statusDetail || "");
    local.set("userId", userId || "");
    local.set("valor", typeof valor === "number" ? valor : 0);
    local.set("metodo", metodo || "unknown");
    if (externalReference) local.set("externalReference", externalReference);
    await local.save(null, { useMasterKey: true });
    return local;
  }

  local.set("status", status || local.get("status") || "unknown");
  if (statusDetail) local.set("statusDetail", statusDetail);
  if (userId && !local.get("userId")) local.set("userId", userId);
  if (typeof valor === "number" && !local.get("valor")) local.set("valor", valor);
  if (metodo && !local.get("metodo")) local.set("metodo", metodo);
  if (externalReference && !local.get("externalReference")) local.set("externalReference", externalReference);

  await local.save(null, { useMasterKey: true });
  return local;
}

// =====================================================
// Rotas
// =====================================================
app.get("/pagamento", (_, res) => res.status(200).send("✅ Webhook OK"));

app.post("/pagamento", async (req, res) => {
  const paymentIdRaw = req.body?.data?.id || req.body?.resource || null;
  const paymentId = toInt(paymentIdRaw);

  if (!paymentId) {
    console.error("❌ Notificação sem paymentId:", req.body);
    return res.sendStatus(400);
  }

  try {
    const pagamento = await mpGetPayment(paymentId);

    const status = String(pagamento.status || "unknown");
    const statusDetail = String(pagamento.status_detail || "");
    const valor = typeof pagamento.transaction_amount === "number" ? pagamento.transaction_amount : 0;

    const externalReference = String(pagamento.external_reference || "").trim();

    const userIdFromMeta = pagamento?.metadata?.user_id ? String(pagamento.metadata.user_id).trim() : "";
    const userIdFinal =
      userIdFromMeta ||
      (externalReference ? parseUserIdFromExternalRef(externalReference) : "");

    const pmId = String(pagamento.payment_method_id || "");
    let metodo = "unknown";
    if (pmId === "pix") metodo = "pix";
    else if (pmId.startsWith("bol")) metodo = "boleto";
    else if (pmId) metodo = "card";

    const local = await upsertPaymentV2({
      mpPaymentId: paymentId,
      status,
      statusDetail,
      userId: userIdFinal || "",
      valor,
      metodo,
      externalReference,
    });

    if (status === "approved") {
      if (!userIdFinal) {
        console.warn("⚠️ approved sem userId. mpPaymentId:", paymentId);
        return res.sendStatus(200);
      }

      if (local.get("credited") === true) return res.sendStatus(200);

      const referencia = `mp:${paymentId}`;
      const credit = await creditSaldoIdempotente({ userId: userIdFinal, valor, referencia });

      local.set("credited", true);
      local.set("creditedAt", new Date());
      local.set("creditDuplicated", credit.duplicated === true);
      await local.save(null, { useMasterKey: true });

      console.log("✅ Crédito aplicado:", { paymentId, userIdFinal, valor, duplicated: credit.duplicated });
    }

    return res.sendStatus(200);
  } catch (err) {
    const msg = err?.response?.data ? JSON.stringify(err.response.data) : (err?.message || String(err));
    console.error("❌ Erro no webhook:", msg);
    return res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Webhook ativo na porta ${PORT} (MP_MODE=${MP_MODE})`));
