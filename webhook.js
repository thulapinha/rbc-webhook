// File: webhook.js
// Express webhook para Mercado Pago (Render)
// ✅ ESM (package.json com "type": "module")
// ✅ Idempotente (não credita 2x)
// ✅ Atualiza PaymentsV2 no Parse
// ✅ Extrai userId por metadata.user_id (novo) e fallback no external_reference (legado)
// ✅ Suporta notificações payment + merchant_order + variações de payload
// ✅ Rastreio forte (requestId + logs consistentes + endpoint /diag)

import express from "express";
import axios from "axios";
import Parse from "parse/node.js";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));


/**
 * 🧠 Anti-duplicação (mesmo paymentId chegando 2x quase junto)
 * Render free normalmente roda 1 instância — isso resolve praticamente todos os duplos.
 */
const _locks = new Map(); // key -> expiresAt
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
// =====================================================
// 🔐 CONFIG — ENV primeiro, fallback (se você insistir)
// =====================================================

function maskSecret(v) {
  const s = (v || "").toString().trim();
  if (!s) return "";
  if (s.length <= 10) return "***";
  return `${s.slice(0, 6)}***${s.slice(-4)}`;
}

function resolveMpMode() {
  const raw = (process.env.MP_MODE || "").toString().trim().toLowerCase();
  if (raw === "prod" || raw === "production" || raw === "live") return "prod";
  if (raw === "test" || raw === "sandbox") return "test";

  // auto-detect pelo token que existir no ENV
  const prodToken =
    (process.env.MP_ACCESS_TOKEN || process.env.MP_ACCESS_TOKEN_PROD || process.env.MP_ACCESS_TOKEN_LIVE || "").toString().trim();
  const testToken = (process.env.MP_ACCESS_TOKEN_TEST || "").toString().trim();

  if (prodToken.startsWith("APP_USR-")) return "prod";
  if (testToken.startsWith("TEST-")) return "test";

  // fallback: se tiver só um token, decide pelo prefixo
  const any =
    (process.env.MP_ACCESS_TOKEN || process.env.MP_ACCESS_TOKEN_TEST || "").toString().trim();
  if (any.startsWith("APP_USR-")) return "prod";
  if (any.startsWith("TEST-")) return "test";

  return "test"; // padrão seguro
}

const MP_MODE = resolveMpMode(); // "test" | "prod"

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

  // 🚨 trava anti-mistura (economiza dor)
  if (mode === "test" && token.startsWith("APP_USR-")) {
    throw new Error("MP_MODE=test mas accessToken é LIVE (APP_USR). Troque por TEST- (credenciais de teste).");
  }
  if (mode === "prod" && token.startsWith("TEST-")) {
    throw new Error("MP_MODE=prod mas accessToken é TEST-. Troque por APP_USR (produção).");
  }
  if (!token) {
    throw new Error("MP access token vazio (MP_ACCESS_TOKEN / MP_ACCESS_TOKEN_TEST).");
  }

  return token;
}

// Parse (ENV no Render, fallback no código)
const PARSE_CFG = {
  serverURL: (process.env.PARSE_SERVER_URL || process.env.PARSE_SERVER || "").trim(),
  appId: (process.env.PARSE_APP_ID || "").trim(),
  masterKey: (process.env.PARSE_MASTER_KEY || "").trim(),
};

function assertEnv(name, value) {
  if (!value) throw new Error(`${name} vazio. Configure no ENV do Render.`);
  return value;
}

// valida env cedo
assertEnv("PARSE_SERVER_URL", PARSE_CFG.serverURL);
assertEnv("PARSE_APP_ID", PARSE_CFG.appId);
assertEnv("PARSE_MASTER_KEY", PARSE_CFG.masterKey);


Parse.initialize(PARSE_CFG.appId, PARSE_CFG.jsKey, PARSE_CFG.masterKey);
Parse.serverURL = PARSE_CFG.serverURL;

// =====================================================
// Rastreio
// =====================================================
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

// =====================================================
// Helpers
// =====================================================
function toInt(x) {
  if (typeof x === "number") return Number.isFinite(x) ? x : null;
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

function pick(obj, ...keys) {
  for (const k of keys) {
    if (!k) continue;
    if (Object.prototype.hasOwnProperty.call(obj || {}, k)) return obj[k];
  }
  return undefined;
}

function extractIdFromUrl(url) {
  const s = (url || "").toString();
  const m1 = s.match(/\/v1\/payments\/(\d+)/);
  if (m1) return toInt(m1[1]);
  const m2 = s.match(/\/payments\/(\d+)/);
  if (m2) return toInt(m2[1]);
  return null;
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

async function upsertPaymentV2({ mpPaymentId, status, statusDetail, userId, valor, metodo, externalReference, requestId }) {
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
    local.set("webhookLastRequestId", requestId || "");
    await local.save(null, { useMasterKey: true });
    return local;
  }

  local.set("status", status || local.get("status") || "unknown");
  if (statusDetail) local.set("statusDetail", statusDetail);
  if (userId && !local.get("userId")) local.set("userId", userId);
  if (typeof valor === "number" && !local.get("valor")) local.set("valor", valor);
  if (metodo && !local.get("metodo")) local.set("metodo", metodo);
  if (externalReference && !local.get("externalReference")) local.set("externalReference", externalReference);
  local.set("webhookLastRequestId", requestId || "");

  await local.save(null, { useMasterKey: true });
  return local;
}

function normalizeTopic(t) {
  return (t || "").toString().trim().toLowerCase();
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

  const dataId =
    body?.data?.id ??
    pick(body, "id") ??
    pick(query, "id");

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

async function processPaymentId(paymentId, requestId) {
  const pagamento = await mpGetPayment(paymentId, requestId);

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

  log("PAYMENT_PARSED", {
    requestId,
    paymentId,
    status,
    statusDetail,
    valor,
    metodo,
    paymentMethodId,
    userIdFinal: userIdFinal || null,
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
    requestId,
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
    const credit = await creditSaldoIdempotente({ userId: userIdFinal, valor, referencia });

    local.set("credited", true);
    local.set("creditedAt", new Date());
    local.set("creditDuplicated", credit.duplicated === true);
    await local.save(null, { useMasterKey: true });

    log("CREDIT_APPLIED", { requestId, paymentId, userIdFinal, valor, duplicated: credit.duplicated });
  }
}

// =====================================================
// Rotas
// =====================================================
app.get("/pagamento", (_req, res) => res.status(200).send("✅ Webhook OK (GET)"));

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
      render: ["MP_ACCESS_TOKEN", "MP_MODE", "PARSE_APP_ID", "PARSE_JS_KEY", "PARSE_MASTER_KEY", "PARSE_SERVER_URL"],
      back4app_cloud: ["MP_ACCESS_TOKEN", "MP_PUBLIC_KEY", "MP_MODE", "MP_NOTIFICATION_URL"],
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

  // ✅ sempre 200 (MP para de insistir). Rastreio fica no log.
  res.sendStatus(200);

  try {
    const ids = new Set();

    const directId = toInt(meta.dataId);
    if (directId) ids.add(directId);

    const fromUrl = extractIdFromUrl(meta.resource);
    if (fromUrl) ids.add(fromUrl);

    const isMerchantOrder =
      meta.topic.includes("merchant") ||
      meta.topic.includes("pedido") || // seu log mostrou "pedido_do_comerciante"
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
        log("MERCHANT_ORDER_FAIL", { requestId, error: errToObj(e) });
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
        log("PROCESS_PAYMENT_FAIL", { requestId, paymentId: pid, error: errToObj(e) });
      }
    }
  } catch (e) {
    log("WEBHOOK_FATAL", { requestId, error: errToObj(e) });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  log("START", {
    port: PORT,
    mpMode: MP_MODE,
    mpTokenMasked: maskSecret((MP_KEYS[MP_MODE === "prod" ? "prod" : "test"].accessToken || "").trim()),
    parseServerURL: PARSE_CFG.serverURL,
  });
});
