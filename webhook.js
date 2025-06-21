const express = require('express');
const axios = require('axios');
const Parse = require('parse/node');

const app = express();
app.use(express.json());

// ✅ Credenciais de produção – verifique se o token foi copiado exatamente:
const MP_ACCESS_TOKEN = "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315";
const PARSE_APP_ID = "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz";
const PARSE_JS_KEY = "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw";
const PARSE_SERVER_URL = "https://parseapi.back4app.com";

// 🔧 Inicializa o Parse
Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// GET para teste do webhook
app.get('/pagamento', (req, res) => {
  res.status(200).send("✅ Webhook OK (GET)");
});

// POST que recebe as notificações do Mercado Pago
app.post('/pagamento', async (req, res) => {
  console.log("🔔 Notificação recebida:", req.body);

  // 📌 Extrai o paymentId de forma flexível
  const paymentId =
    req.body?.data?.id ||
    (req.body?.resource && req.body.topic === "payment" ? req.body.resource : null);

  if (!paymentId) {
    console.error("❌ Sem paymentId na notificação.");
    return res.sendStatus(400);
  }

  try {
    // Consulta a API do Mercado Pago com o token de produção
    const { data: pagamento } = await axios.get(
      `https://api.mercadopago.com/v1/payments/${paymentId}`,
      {
        headers: {
          Authorization: `Bearer ${MP_ACCESS_TOKEN}`
        }
      }
    );

    console.log("📦 Dados do pagamento:", pagamento);

    if (pagamento.status === "approved") {
      const userId = pagamento.external_reference;
      const valor = pagamento.transaction_amount;

      if (!userId || !valor) {
        console.warn("⚠️ Pagamento aprovado mas faltando userId ou valor.");
        return res.sendStatus(400);
      }

      console.log(`✅ Pagamento aprovado: R$${valor} para userId: ${userId}`);

      try {
        await Parse.Cloud.run("addSaldo", {
          userId,
          valor,
          referencia: pagamento.id
        });
        console.log("🪙 Saldo atualizado com sucesso!");
      } catch (cloudError) {
        console.error("❌ Erro na Cloud Function addSaldo:", cloudError);
      }
    } else {
      console.log(`ℹ️ Pagamento com status: ${pagamento.status}`);
    }

    res.sendStatus(200);
  } catch (error) {
    // Exibe detalhes do erro para identificar o 401
    console.error("❌ Erro ao consultar pagamento:", error.response ? error.response.data : error.message);
    res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Webhook ativo na porta ${PORT}`));
