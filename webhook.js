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

function safeLower(value) {
  return (value == null ? "" : String(value)).trim().toLowerCase();
}

function truthyFlag(value) {
  if (value === true) return true;
  const s = safeLower(value);
  return s === "true" || s === "1" || s === "yes" || s === "sim";
}

function looksLikeSubscriptionRef(value) {
  const s = safeLower(value);
  if (!s) return false;
  return s.startsWith("subscription:") ||
    s.includes("rbc_subscription") ||
    s.includes("subscription_payment") ||
    s.includes("assinatura") ||
    s.includes("subscription");
}

function extractSubscriptionPaymentIdFromRef(value) {
  const s = (value || "").toString().trim();
  const m = s.match(/^subscription:([A-Za-z0-9]{10})$/i);
  if (m) return m[1];
  return "";
}

function isSubscriptionPaymentFromMp(pagamento) {
  const meta = pagamento?.metadata || {};
  const externalReference = (pagamento?.external_reference || "").toString().trim();
  return looksLikeSubscriptionRef(externalReference) ||
    safeLower(meta?.purpose) === "rbc_subscription" ||
    safeLower(meta?.payment_purpose) === "subscription" ||
    truthyFlag(meta?.is_subscription_payment) ||
    truthyFlag(meta?.do_not_credit_saldo) ||
    meta?.subscription_payment_id != null;
}

function subscriptionPaymentIdFromMp(pagamento) {
  const meta = pagamento?.metadata || {};
  const fromMeta = (meta?.subscription_payment_id || "").toString().trim();
  if (fromMeta) return fromMeta;
  return extractSubscriptionPaymentIdFromRef(pagamento?.external_reference || "");
}

function parseUserIdFromExternalRef(externalRef) {
  const s = (externalRef || "").toString().trim();

  // Assinatura NÃO é userId. Ex.: subscription:wDDs3tt1ZR
  if (looksLikeSubscriptionRef(s)) return null;

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

  if (await isSubscriptionCreditReference({ referencia, userId, valor })) {
    log("SUBSCRIPTION_CREDIT_BLOCKED", { userId, valor, referencia, novoSaldo: saldoAtual });
    return {
      duplicated: false,
      ignored: true,
      ignoredReason: "subscription_payment_does_not_credit_saldo",
      novoSaldo: saldoAtual,
    };
  }

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

async function findSubscriptionPaymentObject({
  subscriptionPaymentId = "",
  mpPaymentId = null,
  externalReference = "",
  userId = "",
  amount = null,
}) {
  const queries = [];
  const cleanSubId = (subscriptionPaymentId || "").toString().trim();
  const cleanExternal = (externalReference || "").toString().trim();

  if (cleanSubId) {
    const qId = new Parse.Query("RbcSubscriptionPayment");
    try {
      const byId = await qId.get(cleanSubId, { useMasterKey: true });
      if (byId) return byId;
    } catch (_) {}

    const qSubExt = new Parse.Query("RbcSubscriptionPayment");
    qSubExt.equalTo("externalReference", `subscription:${cleanSubId}`);
    queries.push(qSubExt);
  }

  if (cleanExternal) {
    const qExt = new Parse.Query("RbcSubscriptionPayment");
    qExt.equalTo("externalReference", cleanExternal);
    queries.push(qExt);
  }

  if (mpPaymentId != null) {
    const qMp = new Parse.Query("RbcSubscriptionPayment");
    qMp.equalTo("mpPaymentId", Number(mpPaymentId));
    queries.push(qMp);
  }

  if (userId && amount != null) {
    const qUser = new Parse.Query("RbcSubscriptionPayment");
    qUser.equalTo("userId", String(userId).trim());
    qUser.equalTo("amount", Number(amount));
    qUser.containedIn("status", ["pending", "in_process", "approved", "paid"]);
    qUser.descending("createdAt");
    queries.push(qUser);
  }

  if (!queries.length) return null;
  const q = queries.length === 1 ? queries[0] : Parse.Query.or(...queries);
  q.descending("createdAt");
  q.limit(1);
  return await q.first({ useMasterKey: true });
}

async function upsertSubscriptionPaymentFromMp({
  pagamento,
  paymentId,
  status,
  statusDetail,
  valor,
  externalReference,
  userId,
  requestId,
  approvedAt,
  paidAt,
  mpLastUpdatedAt,
  mpPaymentMethodId,
  mpPaymentTypeId,
}) {
  const subId = subscriptionPaymentIdFromMp(pagamento);
  const local = await findSubscriptionPaymentObject({
    subscriptionPaymentId: subId,
    mpPaymentId: paymentId,
    externalReference,
    userId,
    amount: valor,
  });

  if (!local) {
    log("SUBSCRIPTION_PAYMENT_NOT_FOUND", {
      requestId,
      paymentId,
      subscriptionPaymentId: subId || null,
      externalReference: externalReference || null,
      userId: userId || null,
      valor,
    });
    return null;
  }

  const tdata = pagamento?.point_of_interaction?.transaction_data || {};
  local.set("mpPaymentId", paymentId);
  local.set("status", status || "unknown");
  if (statusDetail) local.set("statusDetail", statusDetail);
  if (typeof valor === "number" && valor > 0) {
    local.set("amount", valor);
    local.set("valor", valor);
  }
  if (externalReference) local.set("externalReference", externalReference);
  if (userId && !local.get("userId")) local.set("userId", userId);
  local.set("paymentPurpose", "subscription");
  local.set("isSubscriptionPayment", true);
  local.set("doNotCreditSaldo", true);
  local.set("creditToUser", false);
  local.set("financialOrigin", "ASSINATURA_PLANO");
  if (mpPaymentMethodId) local.set("mpPaymentMethodId", mpPaymentMethodId);
  if (mpPaymentTypeId) local.set("mpPaymentTypeId", mpPaymentTypeId);
  if (approvedAt) local.set("approvedAt", approvedAt);
  if (paidAt) local.set("paidAt", paidAt);
  if (mpLastUpdatedAt) local.set("mpLastUpdatedAt", mpLastUpdatedAt);
  if (tdata?.qr_code) local.set("qrCodeText", String(tdata.qr_code));
  if (tdata?.qr_code_base64) local.set("qrCodeBase64", String(tdata.qr_code_base64));
  if (tdata?.ticket_url) local.set("ticketUrl", String(tdata.ticket_url));
  local.set("webhookLastRequestId", requestId || "");
  local.set("webhookSource", "render_mercado_pago_subscription_guard");
  await local.save(null, { useMasterKey: true });
  return local;
}

async function isSubscriptionCreditReference({ referencia, userId, valor }) {
  if (looksLikeSubscriptionRef(referencia)) return true;
  const raw = (referencia || "").toString().trim();
  const clean = raw.replace(/^ref:/i, "").replace(/^mp:/i, "").trim();
  const mpId = toInt(clean);
  const found = await findSubscriptionPaymentObject({
    mpPaymentId: mpId,
    externalReference: raw,
    userId,
    amount: valor,
  });
  return !!found;
}

async function masterUsersForPush() {
  const masterEmails = (process.env.RBC_MASTER_EMAILS || "rbcservico32@gmail.com")
    .split(",")
    .map((x) => x.trim().toLowerCase())
    .filter(Boolean);

  const queries = [];
  if (masterEmails.length) {
    const byEmail = new Parse.Query(Parse.User);
    byEmail.containedIn("email", masterEmails);
    queries.push(byEmail);

    const byUsername = new Parse.Query(Parse.User);
    byUsername.containedIn("username", masterEmails);
    queries.push(byUsername);
  }

  for (const field of ["role", "adminRole", "perfil", "accountType", "tipoConta"]) {
    const q = new Parse.Query(Parse.User);
    q.containedIn(field, ["master", "MASTER", "admin_master", "ADMIN_MASTER"]);
    queries.push(q);
  }

  const qMaster = new Parse.Query(Parse.User);
  qMaster.equalTo("isMaster", true);
  queries.push(qMaster);

  const qAdminMaster = new Parse.Query(Parse.User);
  qAdminMaster.equalTo("isAdminMaster", true);
  queries.push(qAdminMaster);

  const q = queries.length === 1 ? queries[0] : Parse.Query.or(...queries);
  q.limit(200);
  return await q.find({ useMasterKey: true });
}

async function notifyMasterSubscription({ subscriptionPayment, pagamento, requestId }) {
  if (!subscriptionPayment) return;
  const dedupeKey = `subscription_admin:${subscriptionPayment.id}`;

  const oldQ = new Parse.Query("Notifications");
  oldQ.equalTo("dedupeKey", dedupeKey);
  const existing = await oldQ.first({ useMasterKey: true });
  if (!existing) {
    const plan = (subscriptionPayment.get("planLabel") || subscriptionPayment.get("plan") || "Assinatura").toString();
    const amount = Number(subscriptionPayment.get("amount") || pagamento?.transaction_amount || 0);
    const email = (subscriptionPayment.get("email") || pagamento?.payer?.email || "").toString();
    const nome = (subscriptionPayment.get("nome") || subscriptionPayment.get("name") || email || "Cliente").toString();
    const body = `${nome} assinou ${plan} por R$ ${amount.toFixed(2).replace(".", ",")}.`;

    const note = new Parse.Object("Notifications");
    note.set("title", "Nova assinatura RBC");
    note.set("body", body);
    note.set("level", "info");
    note.set("audience", "admin");
    note.set("targetPlatform", "mobile");
    note.set("sendPush", false);
    note.set("read", false);
    note.set("kind", "subscription_admin_ok");
    note.set("route", "/admin/subscriptions");
    note.set("dedupeKey", dedupeKey);
    note.set("subscriptionPaymentId", subscriptionPayment.id);
    note.set("sourceUserId", subscriptionPayment.get("userId") || "");
    note.set("email", email);
    note.set("amount", String(amount));
    note.set("valor", amount);
    note.set("mpPaymentId", pagamento?.id || null);
    await note.save(null, { useMasterKey: true });
  }

  if (subscriptionPayment.get("renderMasterPushAt")) return;

  const masters = await masterUsersForPush();
  const ids = masters.map((u) => u.id).filter(Boolean);
  if (!ids.length) {
    log("SUBSCRIPTION_MASTER_PUSH_NO_MASTER", { requestId, subscriptionPaymentId: subscriptionPayment.id });
    return;
  }

  const q1 = new Parse.Query(Parse.Installation);
  q1.equalTo("isMaster", true);

  const q2 = new Parse.Query(Parse.Installation);
  q2.containedIn("userId", ids);

  const q3 = new Parse.Query(Parse.Installation);
  q3.containedIn("user", masters);

  const where = Parse.Query.or(q1, q2, q3);
  const amount = Number(subscriptionPayment.get("amount") || pagamento?.transaction_amount || 0);
  const plan = (subscriptionPayment.get("planLabel") || subscriptionPayment.get("plan") || "Assinatura").toString();
  const email = (subscriptionPayment.get("email") || pagamento?.payer?.email || "").toString();

  await Parse.Push.send({
    where,
    data: {
      title: "Nova assinatura RBC",
      alert: `${email || "Cliente"} assinou ${plan} por R$ ${amount.toFixed(2).replace(".", ",")}.`,
      body: `${email || "Cliente"} assinou ${plan} por R$ ${amount.toFixed(2).replace(".", ",")}.`,
      sound: "moeda",
      android_channel_id: "rbc_high_importance_channel_moeda_v1",
      channel_id: "rbc_high_importance_channel_moeda_v1",
      notification_channel_id: "rbc_high_importance_channel_moeda_v1",
      badge: "Increment",
      type: "subscription_admin_ok",
      deep_link: "/admin/subscriptions",
      click_action: "FLUTTER_NOTIFICATION_CLICK",
      paymentId: subscriptionPayment.id,
      subscriptionPaymentId: subscriptionPayment.id,
      mpPaymentId: pagamento?.id || null,
      amount: String(amount),
      valor: amount,
      notificationScope: "master_only",
    },
  }, { useMasterKey: true });

  subscriptionPayment.set("renderMasterPushAt", new Date());
  await subscriptionPayment.save(null, { useMasterKey: true });
}


async function notifyMasterDeposit({ local, pagamento, userId, valor, metodo, requestId }) {
  if (!local) return;
  if (local.get("renderMasterDirectPushAt") || local.get("pushAdminDirectNotifiedAt")) return;

  const amount = Number(valor || local.get("valor") || pagamento?.transaction_amount || 0);
  const user = userId ? await new Parse.Query(Parse.User).get(String(userId), { useMasterKey: true }).catch(() => null) : null;
  const nome = (user?.get("name") || user?.get("nome") || local.get("nome") || local.get("payerName") || user?.get("email") || local.get("email") || "Cliente").toString();
  const email = (user?.get("email") || local.get("email") || pagamento?.payer?.email || "").toString();
  const method = (metodo || local.get("metodo") || pagamento?.payment_method_id || "pix").toString().toUpperCase();
  const mpPaymentId = pagamento?.id || local.get("mpPaymentId") || null;
  const dedupeKey = `payment_admin:${local.id || mpPaymentId || requestId}`;
  const body = `${nome} depositou R$ ${amount.toFixed(2).replace(".", ",")} via ${method}.`;

  // Reserva flags antes do save final para o afterSave do Back4App não mandar duplicado.
  const now = new Date();
  local.set("adminInboxCreatedAt", now);
  local.set("pushAdminDirectNotifiedAt", now);
  local.set("pushAdminNotifiedAt", now);
  local.set("renderMasterDirectPushAt", now);
  local.set("lastMasterDepositPushBody", body);

  const oldQ = new Parse.Query("Notifications");
  oldQ.equalTo("dedupeKey", dedupeKey);
  const existing = await oldQ.first({ useMasterKey: true });
  if (!existing) {
    const note = new Parse.Object("Notifications");
    note.set("title", "Pagamento recebido");
    note.set("body", body);
    note.set("level", "info");
    note.set("audience", "admin");
    note.set("targetPlatform", "mobile");
    note.set("sendPush", false);
    note.set("read", false);
    note.set("kind", "payment_admin_ok");
    note.set("route", "/adminPanel");
    note.set("dedupeKey", dedupeKey);
    note.set("paymentObjectId", local.id || "");
    note.set("sourceUserId", userId || "");
    note.set("email", email);
    note.set("customerName", nome);
    note.set("payerName", nome);
    note.set("metodo", method);
    note.set("amount", String(amount));
    note.set("valor", amount);
    note.set("paidAmount", amount);
    note.set("transaction_amount", amount);
    note.set("mpPaymentId", mpPaymentId);
    await note.save(null, { useMasterKey: true });
  }

  const masters = await masterUsersForPush();
  const ids = masters.map((u) => u.id).filter(Boolean);
  if (!ids.length) {
    log("DEPOSIT_MASTER_PUSH_NO_MASTER", { requestId, paymentId: local.id || null, mpPaymentId });
    return;
  }

  const q1 = new Parse.Query(Parse.Installation);
  q1.equalTo("isMaster", true);

  const q2 = new Parse.Query(Parse.Installation);
  q2.containedIn("userId", ids);

  const q3 = new Parse.Query(Parse.Installation);
  q3.containedIn("user", masters);

  const where = Parse.Query.or(q1, q2, q3);
  await Parse.Push.send({
    where,
    data: {
      title: "Pagamento recebido",
      alert: body,
      body,
      sound: "moeda",
      android_channel_id: "rbc_high_importance_channel_moeda_v1",
      channel_id: "rbc_high_importance_channel_moeda_v1",
      notification_channel_id: "rbc_high_importance_channel_moeda_v1",
      badge: "Increment",
      type: "payment_admin_ok",
      deep_link: "/adminPanel",
      click_action: "FLUTTER_NOTIFICATION_CLICK",
      paymentObjectId: local.id || "",
      paymentId: local.id || "",
      mpPaymentId,
      userId: userId || "",
      sourceUserId: userId || "",
      customerName: nome,
      payerName: nome,
      name: nome,
      email,
      metodo: method,
      method,
      amount: String(amount),
      valor: amount,
      value: amount,
      paidAmount: amount,
      transaction_amount: amount,
      dedupeKey,
      notificationScope: "master_only",
    },
  }, { useMasterKey: true });

  log("DEPOSIT_MASTER_PUSH_SENT", { requestId, paymentObjectId: local.id || null, mpPaymentId, userId, amount, masterCount: ids.length });
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

  if (isSubscriptionPaymentFromMp(pagamento)) {
    const subPayment = await upsertSubscriptionPaymentFromMp({
      pagamento,
      paymentId,
      status,
      statusDetail,
      valor,
      externalReference,
      userId: userIdFinal || "",
      requestId,
      approvedAt,
      paidAt,
      mpLastUpdatedAt,
      mpPaymentMethodId: paymentMethodId,
      mpPaymentTypeId: paymentTypeId,
    });

    log("SUBSCRIPTION_PAYMENT_ROUTED", {
      requestId,
      paymentId,
      status,
      userIdFinal: userIdFinal || null,
      externalReference: externalReference || null,
      subscriptionPaymentId: subPayment?.id || subscriptionPaymentIdFromMp(pagamento) || null,
      credited: false,
    });

    if (status === "approved" && subPayment) {
      try {
        await notifyMasterSubscription({ subscriptionPayment: subPayment, pagamento, requestId });
      } catch (e) {
        log("SUBSCRIPTION_MASTER_NOTIFY_FAIL", { requestId, paymentId, error: errToObj(e) });
      }
    }

    return;
  }

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

    try {
      await notifyMasterDeposit({ local, pagamento, userId: userIdFinal, valor, metodo, requestId });
    } catch (e) {
      log("DEPOSIT_MASTER_NOTIFY_FAIL", { requestId, paymentId, error: errToObj(e) });
    }

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
    subscriptionGuard: "enabled-no-credit-to-saldo-v1",
    masterDepositPushGuard: "enabled-render-master-direct-payment-push-v1",
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
