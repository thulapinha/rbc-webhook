// File: webhook.js
// Express webhook para Mercado Pago (Render)
// ✅ ESM (package.json com "type": "module")
// ✅ Idempotente (não credita 2x)
// ✅ Atualiza PaymentsV2 no Parse
// ✅ Extrai userId por metadata.user_id (novo) e fallback no external_reference (legado)

import express from "express";
import axios from "axios";
import Parse from "parse/node.js";

const app = express();
app.use(express.json());

// =====================================================
// 🔐 CONFIG — USE ENV (preferível) + fallback
// =====================================================
const MP_MODE = (process.env.MP_MODE || "test").trim().toLowerCase(); // "test" | "prod"

const MP_KEYS = {
  test: {
    accessToken: (process.env.MP_ACCESS_TOKEN_TEST || "TEST-APP_USR-712868030410210-012422-c7031be0b237288c2ffe5c809e99e5f7-2510340016").trim(),
  },
  prod: {
    accessToken: (process.env.MP_ACCESS_TOKEN || "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315").trim(),
  },
};

function mpAccessToken() {
  const mode = MP_MODE === "prod" ? "prod" : "test";
  const token = (MP_KEYS[mode].accessToken || "").trim();

  // 🚨 trava anti-mistura (economiza dor)
  if (mode === "test" && token.startsWith("APP_USR-")) {
    throw new Error("MP_MODE=test mas accessToken é LIVE (APP_USR). Troque por TEST- (credenciais de teste).");
  }
  if (mode === "prod" && token.startsWith("TEST-")) {
    throw new Error("MP_MODE=prod mas accessToken é TEST-. Troque por APP_USR (produção).");
  }

  return token;
}

// ✅ Credenciais de produção – verifique se os valores estão exatamente corretos:
const MP_ACCESS_TOKEN = "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315";
const PARSE_APP_ID     = "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz";
const PARSE_JS_KEY     = "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw";
const PARSE_MASTER_KEY = "Ou385YEpEfoT3gZ6hLSbTfKZYQtTgNA7WNBnv7ia";
const PARSE_SERVER_URL = "https://parseapi.back4app.com";
// Init Parse
Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY, PARSE_MASTER_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// =====================================================
// Helpers
// =====================================================
function toInt(x) {
  if (typeof x === "number") return x;
  if (typeof x === "string") {
    const n = parseInt(x, 10);
    return Number.isFinite(n) ? n : null;
  }
  return null;
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
  if (!token) throw new Error("MP access token não configurado no webhook.");

  const { data } = await axios.get(`https://api.mercadopago.com/v1/payments/${paymentId}`, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: 15000,
  });
  return data;
}

async function creditSaldoIdempotente({ userId, valor, referencia }) {
  const userQuery = new Parse.Query(Parse.User);
  userQuery.equalTo("objectId", userId);
  const user = await userQuery.first({ useMasterKey: true });
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
app.get("/pagamento", (_req, res) => res.status(200).send("✅ Webhook OK (GET)"));

app.post("/pagamento", async (req, res) => {
  const paymentIdRaw =
    req.body?.data?.id ||
    (req.body?.resource && req.body?.topic === "payment" ? req.body.resource : null);

  const paymentId = toInt(paymentIdRaw);
  if (!paymentId) {
    console.error("❌ Sem paymentId válido na notificação:", req.body);
    return res.sendStatus(400);
  }

  try {
    const pagamento = await mpGetPayment(paymentId);

    const status = (pagamento.status || "unknown").toString();
    const statusDetail = (pagamento.status_detail || "").toString();
    const valor = typeof pagamento.transaction_amount === "number" ? pagamento.transaction_amount : 0;

    const userId =
      (pagamento.metadata && pagamento.metadata.user_id) ||
      pagamento.external_reference ||
      null;

    const userIdFinal =
      (userId && userId.toString().trim())
        ? userId.toString().trim()
        : parseUserIdFromExternalRef(pagamento.external_reference);

    const paymentMethodId = (pagamento.payment_method_id || "").toString();
    let metodo = "unknown";
    if (paymentMethodId === "pix") metodo = "pix";
    else if (paymentMethodId.startsWith("bol")) metodo = "boleto";
    else if (paymentMethodId) metodo = "card";

    const externalReference = (pagamento.external_reference || "").toString().trim();

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
        console.warn("⚠️ Pagamento aprovado, mas sem userId para crédito. paymentId=", paymentId);
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
  } catch (error) {
    const msg = error?.response?.data ? JSON.stringify(error.response.data) : (error?.message || String(error));
    console.error("❌ Erro no webhook:", msg);
    return res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`🚀 Webhook ativo na porta ${PORT} (MP_MODE=${MP_MODE})`));
