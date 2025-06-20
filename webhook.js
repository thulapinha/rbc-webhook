const express = require('express');
const axios = require('axios');
const Parse = require('parse/node');

const app = express();
app.use(express.json());

// Credenciais (use as que você já configurou)
const MP_ACCESS_TOKEN = "APP_USR-2425109007347629-062014-4aebea93a2ceaa33770018567f062c3-40790315";
const PARSE_APP_ID = "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz";
const PARSE_JS_KEY = "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw";
const PARSE_SERVER_URL = "https://parseapi.back4app.com";

Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// Endpoint GET para testes – assegure que a URL está acessível
app.get('/pagamento', (req, res) => {
  res.status(200).send("Webhook OK (GET)");
});

// Endpoint POST: será chamado pelo Mercado Pago
app.post('/pagamento', async (req, res) => {
  console.log("🔔 Notificação recebida:", req.body);

  // O Mercado Pago envia o ID do pagamento dentro de req.body.data.id
  const paymentId = req.body.data && req.body.data.id;
  if (!paymentId) {
    console.error("❌ Sem paymentId na notificação.");
    return res.sendStatus(400);
  }

  try {
    // Consulta a API do Mercado Pago para obter o status do pagamento
    const { data: pagamento } = await axios.get(
      `https://api.mercadopago.com/v1/payments/${paymentId}`,
      { headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` } }
    );

    console.log("Dados do pagamento:", pagamento);

    if (pagamento.status === "approved") {
      const userId = pagamento.external_reference; // deve ter sido definido na criação da preferência
      const valor = pagamento.transaction_amount;
      console.log(`✅ Pagamento aprovado: R$${valor} para userId: ${userId}`);

      // Chama a função Cloud para atualizar saldo
      try {
        await Parse.Cloud.run("addSaldo", {
          userId,
          valor,
          referencia: pagamento.id
        });
        console.log("🪙 Saldo atualizado com sucesso!");
      } catch (cloudError) {
        console.error("Erro na Cloud Function addSaldo:", cloudError);
      }
    } else {
      console.log("ℹ️ Pagamento não aprovado, status:", pagamento.status);
    }

    // Retorne HTTP 200 para confirmar a recepção da notificação
    res.sendStatus(200);
  } catch (error) {
    console.error("❌ Erro ao processar notificação do MP:", error.message);
    res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Webhook ativo na porta ${PORT}`));
