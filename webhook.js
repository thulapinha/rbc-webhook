// File: webhook.js
// Express webhook para Mercado Pago (Render)
// ✅ ESM (package.json com "type": "module")
// ✅ Idempotente (não credita 2x)
// ✅ Atualiza o PaymentsV2 original no Parse sempre que possível
// ✅ Extrai userId por metadata.user_id (novo) e fallback no external_reference (legado)
// ✅ Suporta notificações payment + merchant_order + variações de payload
// ✅ Rastreio forte (requestId + logs consistentes + endpoint /diag)
// ✅ Compatível com fluxo atual sem quebrar PIX/BOLETO já funcionando

import express from "express";
import axios from "axios";
import Parse from "parse/node.js";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));

/**
 * Anti-duplicação em memória.
 * Em Render com 1 instância isso já segura quase todos os duplos que chegam juntos.
 */
const _locks = new Map();
function acquireLock(key, ttlMs = 5 * 60 * 1000) {
  const now = Date.now();
  const exp = _locks.get(key);
  if (exp && exp > now) return false;
  _locks.set(key, now + ttlMs);
  return true;
}
function lockKeyForCredit(paymentId) {
  const s = String(paymentId || "");
  const h = crypto.createHash("sha1").update(`credit:${s}`).digest("hex");
  return `credit:${h}`;
}

function maskSecret(v) {
  const s = (v || "").toString().trim();
  if (!s) return "";
  if (s.length <= 10) return "***";
  return `${s.slice(0, 6)}***${s.slice(-4)}`;
}

function nowIso() {
  return new Date().toISOString();
}

function rid() {
  return `rid_${Date.now()}_${crypto.randomBytes(6).toString("hex")}`;
}

function log(tag, obj) {
  const safe = obj ? JSON.stringify(obj) : "";
  console.log(`[WEBHOOK] ${nowIso()} | ${tag} ${safe}`);
}

function errToObj(err) {
  const status = err?.response?.status ?? err?.status ?? null;
  const data = err?.response?.data ?? err?.data ?? null;
  const message = err?.message || String(err);
  return { status, message, data };
}

function resolveMpMode() {
  const raw = (process.env.MP_MODE || "").toString().trim().toLowerCase();
  if (raw === "prod" || raw === "production" || raw === "live") return "prod";
  if (raw === "test" || raw === "sandbox") return "test";

  const prodToken = (
    process.env.MP_ACCESS_TOKEN ||
    process.env.MP_ACCESS_TOKEN_PROD ||
    process.env.MP_ACCESS_TOKEN_LIVE ||
    ""
  )
    .toString()
    .trim();

  const testToken = (process.env.MP_ACCESS_TOKEN_TEST || "").toString().trim();

  if (prodToken.startsWith("APP_USR-")) return "prod";
  if (testToken.startsWith("TEST-")) return "test";

  const any = (process.env.MP_ACCESS_TOKEN || process.env.MP_ACCESS_TOKEN_TEST || "")
    .toString()
    .trim();

  if (any.startsWith("APP_USR-")) return "prod";
  if (any.startsWith("TEST-")) return "test";

  return "test";
}

const MP_MODE = resolveMpMode();

const MP_KEYS = {
  test: {
    accessToken: (process.env.MP_ACCESS_TOKEN_TEST || "").trim(),
  },
  prod: {
    accessToken: (process.env.MP_ACCESS_TOKEN || "").trim(),
  },
};

function mpAccessToken() {
  const mode = MP_MODE === "prod" ? "prod" : "test";
  const token = (MP_KEYS[mode].accessToken || "").trim();

  if (mode === "test" && token.startsWith("APP_USR-")) {
    throw new Error("MP_MODE=test mas accessToken é LIVE (APP_USR). Troque por TEST-.");
  }
  if (mode === "prod" && token.startsWith("TEST-")) {
    throw new Error("MP_MODE=prod mas accessToken é TEST-. Troque por APP_USR.");
  }
  if (!token) {
    throw new Error("MP access token vazio (MP_ACCESS_TOKEN / MP_ACCESS_TOKEN_TEST).");
  }

  return token;
}

const PARSE_CFG = {
  serverURL: (process.env.PARSE_SERVER_URL || process.env.PARSE_SERVER || "").trim(),
  appId: (process.env.PARSE_APP_ID || "").trim(),
  jsKey: (process.env.PARSE_JS_KEY || "").trim(),
  masterKey: (process.env.PARSE_MASTER_KEY || "").trim(),
};

function assertEnv(name, value) {
  if (!value) throw new Error(`${name} vazio. Configure no ENV do Render.`);
  return value;
}

assertEnv("PARSE_SERVER_URL", PARSE_CFG.serverURL);
assertEnv("PARSE_APP_ID", PARSE_CFG.appId);
assertEnv("PARSE_MASTER_KEY", PARSE_CFG.masterKey);

Parse.initialize(PARSE_CFG.appId, PARSE_CFG.jsKey || undefined, PARSE_CFG.masterKey);
Parse.serverURL = PARSE_CFG.serverURL;

function toInt(x) {
  if (typeof x === "number") return Number.isFinite(x) ? x : null;
  if (typeof x === "string") {
    const n = parseInt(x, 10);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function normalizeTopic(t) {
  return (t || "").toString().trim().toLowerCase();
}

function pick(obj, ...keys) {
  for (const k of keys) {
    if (!k) continue;
    if (Object.prototype.hasOwnProperty.call(obj || {}, k)) return obj[k];
  }
  return undefined;
}

function parseUserIdFromExternalRef(externalRef) {
  const s = (externalRef || "").toString().trim();

  // Legado formatado: rbc:<userId>:<payId>:<metodo>
  if (s.startsWith("rbc:")) {
    const parts = s.split(":");
    if (parts.length >= 2 && parts[1]) return parts[1];
  }

  // Legado puro: external_reference = userId
  if (s && s.length >= 6 && !looksLikeParseObjectId(s)) return s;

  return null;
}

function looksLikeParseObjectId(value) {
  const s = (value || "").toString().trim();
  return /^[A-Za-z0-9]{10}$/.test(s);
}

function extractIdFromUrl(url) {
  const s = (url || "").toString();
  const m1 = s.match(/\/v1\/payments\/(\d+)/);
  if (m1) return toInt(m1[1]);
  const m2 = s.match(/\/payments\/(\d+)/);
  if (m2) return toInt(m2[1]);
  return null;
}

function extractPayload(req) {
  const body = req.body || {};
  const query = req.query || {};

  const topic =
    pick(body, "topic", "tópico", "topico") ??
    pick(query, "topic", "tópico", "topico");

  const resource =
    pick(body, "resource", "recurso") ??
    pick(query, "resource", "recurso");

  const dataId = body?.data?.id ?? pick(body, "id") ?? pick(query, "id");
  const type = pick(body, "type") ?? pick(query, "type");

  return {
    topic: normalizeTopic(topic),
    resource: resource ? String(resource).trim() : "",
    type: type ? String(type).trim() : "",
    dataId: dataId ?? null,
    bodyKeys: Object.keys(body || {}),
    queryKeys: Object.keys(query || {}),
  };
}

async function mpGetPayment(paymentId, requestId) {
  const token = mpAccessToken();
  const url = `https://api.mercadopago.com/v1/payments/${paymentId}`;

  log("MP_GET_PAYMENT", {
    requestId,
    url,
    mpMode: MP_MODE,
    token: maskSecret(token),
    paymentId,
  });

  const { data } = await axios.get(url, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: 15000,
  });

  return data;
}

async function mpGetMerchantOrder(resourceUrl, requestId) {
  const token = mpAccessToken();
  const url = (resourceUrl || "").toString().trim();
  if (!url) throw new Error("merchant_order sem resource url.");

  log("MP_GET_MERCHANT_ORDER", {
    requestId,
    url,
    mpMode: MP_MODE,
    token: maskSecret(token),
  });

  const { data } = await axios.get(url, {
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

async function findExistingPaymentObject({
  mpPaymentId,
  paymentObjectId,
  externalReference,
}) {
  if (mpPaymentId != null) {
    const q1 = new Parse.Query("PaymentsV2");
    q1.equalTo("mpPaymentId", mpPaymentId);
    const byMpId = await q1.first({ useMasterKey: true });
    if (byMpId) return byMpId;
  }

  if (paymentObjectId) {
    try {
      const q2 = new Parse.Query("PaymentsV2");
      const byObjectId = await q2.get(String(paymentObjectId), { useMasterKey: true });
      if (byObjectId) return byObjectId;
    } catch (_) {}
  }

  if (externalReference) {
    const q3 = new Parse.Query("PaymentsV2");
    q3.equalTo("externalReference", String(externalReference));
    q3.descending("createdAt");
    const byExternalRef = await q3.first({ useMasterKey: true });
    if (byExternalRef) return byExternalRef;
  }

  return null;
}

async function upsertPaymentV2({
  mpPaymentId,
  status,
  statusDetail,
  userId,
  valor,
  metodo,
  externalReference,
  paymentObjectId,
  requestId,
  approvedAt,
  paidAt,
  mpLastUpdatedAt,
  mpPaymentMethodId,
  mpPaymentTypeId,
}) {
  let local = await findExistingPaymentObject({
    mpPaymentId,
    paymentObjectId,
    externalReference,
  });

  if (!local) {
    local = new Parse.Object("PaymentsV2");
  }

  if (mpPaymentId != null) local.set("mpPaymentId", mpPaymentId);
  if (status) local.set("status", status);
  if (statusDetail) local.set("statusDetail", statusDetail);
  if (userId && !local.get("userId")) local.set("userId", userId);
  if (typeof valor === "number" && (!local.get("valor") || local.get("valor") === 0)) {
    local.set("valor", valor);
  }
  if (metodo && !local.get("metodo")) local.set("metodo", metodo);
  if (externalReference && !local.get("externalReference")) {
    local.set("externalReference", externalReference);
  }

  if (approvedAt) local.set("approvedAt", approvedAt);
  if (paidAt) local.set("paidAt", paidAt);
  if (mpLastUpdatedAt) local.set("mpLastUpdatedAt", mpLastUpdatedAt);
  if (mpPaymentMethodId) local.set("mpPaymentMethodId", mpPaymentMethodId);
  if (mpPaymentTypeId) local.set("mpPaymentTypeId", mpPaymentTypeId);

  local.set("webhookLastRequestId", requestId || "");

  await local.save(null, { useMasterKey: true });
  return local;
}

function toDateOrNull(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

async function processPaymentId(paymentId, requestId) {
  const pagamento = await mpGetPayment(paymentId, requestId);

  const status = (pagamento.status || "unknown").toString();
  const statusDetail = (pagamento.status_detail || "").toString();
  const valor =
    typeof pagamento.transaction_amount === "number"
      ? pagamento.transaction_amount
      : 0;

  const metadataUserId =
    pagamento?.metadata?.user_id != null
      ? String(pagamento.metadata.user_id).trim()
      : "";

  const metadataPaymentObjectId =
    pagamento?.metadata?.payment_object_id != null
      ? String(pagamento.metadata.payment_object_id).trim()
      : "";

  const externalReference = (pagamento.external_reference || "").toString().trim();

  const userIdFinal =
    metadataUserId ||
    parseUserIdFromExternalRef(externalReference) ||
    "";

  const paymentMethodId = (pagamento.payment_method_id || "").toString();
  const paymentTypeId = (pagamento.payment_type_id || "").toString();

  let metodo = "unknown";
  if (paymentMethodId === "pix") metodo = "pix";
  else if (paymentMethodId.startsWith("bol")) metodo = "boleto";
  else if (paymentMethodId) metodo = "card";

  const approvedAt = toDateOrNull(pagamento.date_approved);
  const paidAt = toDateOrNull(pagamento.date_of_expiration || pagamento.date_last_updated);
  const mpLastUpdatedAt = toDateOrNull(pagamento.date_last_updated);

  log("PAYMENT_PARSED", {
    requestId,
    paymentId,
    status,
    statusDetail,
    valor,
    metodo,
    paymentMethodId,
    paymentTypeId,
    userIdFinal: userIdFinal || null,
    paymentObjectId: metadataPaymentObjectId || null,
    externalReference: externalReference || null,
  });

  const local = await upsertPaymentV2({
    mpPaymentId: paymentId,
    status,
    statusDetail,
    userId: userIdFinal || "",
    valor,
    metodo,
    externalReference,
    paymentObjectId: metadataPaymentObjectId || "",
    requestId,
    approvedAt,
    paidAt,
    mpLastUpdatedAt,
    mpPaymentMethodId: paymentMethodId,
    mpPaymentTypeId: paymentTypeId,
  });

  if (status === "approved") {
    const lk = lockKeyForCredit(paymentId);
    if (!acquireLock(lk)) {
      log("DUPLICATE_CREDIT_LOCK", { requestId, paymentId, userIdFinal });
      return;
    }

    if (!userIdFinal) {
      log("APPROVED_NO_USERID", { requestId, paymentId });
      return;
    }

    if (local.get("credited") === true) {
      log("ALREADY_CREDITED", { requestId, paymentId, userIdFinal });
      return;
    }

    const referencia = `mp:${paymentId}`;
    const credit = await creditSaldoIdempotente({
      userId: userIdFinal,
      valor,
      referencia,
    });

    local.set("credited", true);
    local.set("creditedAt", new Date());
    local.set("creditDuplicated", credit.duplicated === true);
    await local.save(null, { useMasterKey: true });

    log("CREDIT_APPLIED", {
      requestId,
      paymentId,
      userIdFinal,
      valor,
      duplicated: credit.duplicated,
    });
  }
}

app.get("/pagamento", (_req, res) => {
  res.status(200).send("✅ Webhook OK (GET)");
});

app.get("/diag", (_req, res) => {
  const mode = MP_MODE === "prod" ? "prod" : "test";
  const token = (MP_KEYS[mode].accessToken || "").trim();

  return res.status(200).json({
    ok: true,
    mpMode: mode,
    mpTokenMasked: maskSecret(token),
    parse: {
      serverURL: PARSE_CFG.serverURL,
      appIdMasked: maskSecret(PARSE_CFG.appId),
      jsKeyMasked: maskSecret(PARSE_CFG.jsKey),
      hasMasterKey: !!PARSE_CFG.masterKey,
    },
    expectedEnv: {
      render: [
        "MP_ACCESS_TOKEN",
        "MP_ACCESS_TOKEN_TEST",
        "MP_MODE",
        "PARSE_APP_ID",
        "PARSE_JS_KEY",
        "PARSE_MASTER_KEY",
        "PARSE_SERVER_URL",
      ],
    },
  });
});

app.post("/pagamento", async (req, res) => {
  const requestId = rid();
  const meta = extractPayload(req);

  log("INCOMING", {
    requestId,
    topic: meta.topic,
    type: meta.type,
    resource: meta.resource || null,
    dataId: meta.dataId || null,
    bodyKeys: meta.bodyKeys,
    queryKeys: meta.queryKeys,
  });

  // Responde 200 logo de cara pro MP parar de insistir à toa.
  res.sendStatus(200);

  try {
    const ids = new Set();

    const directId = toInt(meta.dataId);
    if (directId) ids.add(directId);

    const fromUrl = extractIdFromUrl(meta.resource);
    if (fromUrl) ids.add(fromUrl);

    const isMerchantOrder =
      meta.topic.includes("merchant") ||
      meta.topic.includes("pedido") ||
      (meta.resource && meta.resource.includes("merchant_orders"));

    if (isMerchantOrder && meta.resource) {
      try {
        const order = await mpGetMerchantOrder(meta.resource, requestId);
        const pays = Array.isArray(order?.payments) ? order.payments : [];
        for (const p of pays) {
          const pid = toInt(p?.id);
          if (pid) ids.add(pid);
        }
        log("MERCHANT_ORDER_PAYMENTS", {
          requestId,
          merchantOrderId: order?.id ?? null,
          paymentIds: Array.from(ids),
        });
      } catch (e) {
        log("MERCHANT_ORDER_FAIL", {
          requestId,
          error: errToObj(e),
        });
      }
    }

    const list = Array.from(ids).filter(Boolean);

    if (!list.length) {
      log("NO_PAYMENT_ID", { requestId, meta });
      return;
    }

    for (const pid of list) {
      try {
        await processPaymentId(pid, requestId);
      } catch (e) {
        log("PROCESS_PAYMENT_FAIL", {
          requestId,
          paymentId: pid,
          error: errToObj(e),
        });
      }
    }
  } catch (e) {
    log("WEBHOOK_FATAL", {
      requestId,
      error: errToObj(e),
    });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  log("START", {
    port: PORT,
    mpMode: MP_MODE,
    mpTokenMasked: maskSecret(
      (MP_KEYS[MP_MODE === "prod" ? "prod" : "test"].accessToken || "").trim()
    ),
    parseServerURL: PARSE_CFG.serverURL,
  });
});
