import express from 'express';
import axios from 'axios';
import Parse from 'parse/node.js';

const app = express();
app.use(express.json());

// ✅ Credenciais de produção – verifique se os valores estão exatamente corretos:
const MP_ACCESS_TOKEN = "APP_USR-2425109007347629-062014-4aebea93a2ceaaa33770018567f062c3-40790315";
const PARSE_APP_ID     = "Fd6ksAkglKa2CFerh46JHEMOGsqbqXUIRfPOFLOz";
const PARSE_JS_KEY     = "UKqUKChgVWiNIXmMQA1WIkdnjOFrt28cGy68UFWw";
const PARSE_MASTER_KEY = "Ou385YEpEfoT3gZ6hLSbTfKZYQtTgNA7WNBnv7ia";
const PARSE_SERVER_URL = "https://parseapi.back4app.com";

// 🔧 Inicializa o Parse com a Master Key
Parse.initialize(PARSE_APP_ID, PARSE_JS_KEY, PARSE_MASTER_KEY);
Parse.serverURL = PARSE_SERVER_URL;

// GET para teste
app.get('/pagamento', (req, res) => {
  res.status(200).send("✅ Webhook OK (GET)");
});

// POST para notificações de pagamento
app.post('/pagamento', async (req, res) => {
  console.log("🔔 Notificação recebida:", req.body);

  const paymentId =
    req.body?.data?.id ||
    (req.body?.resource && req.body.topic === "payment" ? req.body.resource : null);

  if (!paymentId) {
    console.error("❌ Sem paymentId na notificação.");
    return res.sendStatus(400);
  }

  try {
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
      const valor  = pagamento.transaction_amount;

      if (!userId || !valor) {
        console.warn("⚠️ Pagamento aprovado mas faltando userId ou valor.");
        return res.sendStatus(400);
      }

      console.log(`✅ Pagamento aprovado: R$${valor} para userId: ${userId}`);

      try {
        await Parse.Cloud.run(
          "addSaldo",
          { userId, valor, referencia: pagamento.id },
          { useMasterKey: true }
        );
        console.log("🪙 Saldo atualizado com sucesso!");
      } catch (cloudError) {
        console.error("❌ Erro na Cloud Function addSaldo:", cloudError);
      }
    } else {
      console.log(`ℹ️ Pagamento com status: ${pagamento.status}`);
    }

    res.sendStatus(200);
  } catch (error) {
    console.error("❌ Erro ao consultar pagamento:", error.response?.data || error.message);
    res.sendStatus(500);
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Webhook ativo na porta ${PORT}`);
});
