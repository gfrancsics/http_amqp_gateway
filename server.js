const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

const app = express();

const CLOUDAMQP_URL = 'amqps://vpwdmwwi:5WAHg6cVzwB0duCEeznFHl_W0eqfyF27@cow.rmq2.cloudamqp.com/vpwdmwwi';
const EXCHANGE_NAME = 'email';
let sharedConnection = null;
let sharedChannel = null;

// ----------------------------------------------------
// Függvény a RabbitMQ üzenetküldéshez
// ----------------------------------------------------
async function sendMessage(message) {
    if (!sharedChannel) {
        console.error("❌ Hiba: A RabbitMQ csatorna még nincs inicializálva.");
        return;
    }

    try {
        const messageString = JSON.stringify(message);
        const messageBuffer = Buffer.from(messageString);

        // 1. Exchange deklarálása (marad)
        await sharedChannel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });

        // 2. ÚJ: Sor deklarálása a Producernél is!
        // Fontos: a név (pl. 'hibak_sora') egyezzen a Consumerével!
        const QUEUE_NAME = 'hibajelentesek_sora';
        await sharedChannel.assertQueue(QUEUE_NAME, {
            durable: true,    // Túlélje a restartot
            exclusive: false,
            autoDelete: false // Ne törölje le magát, ha nincs consumer!
        });

        // 3. ÚJ: Összekötjük a sort az exchange-el
        // Ez mondja meg a RabbitMQ-nak, hogy amit az exchange kap, azt tegye ebbe a sorba
        await sharedChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');

        // 4. Publikálás (marad a persistent: true)
        const sent = await sharedChannel.publish(
            EXCHANGE_NAME,
            '',
            messageBuffer,
            { persistent: true }
        );

        if (sent) {
            console.log("✅ Üzenet elküldve.");
        } else {
            console.error("❌ Hiba az üzenet küldésekor (backpressure).");
        }
    } catch (error) {
        console.error("❌ Hiba történt az üzenetküldés során:", error.message);
    }
}

app.use(cors()); // Ez engedélyezi a CORS-t mindenki számára
app.use(express.json());

app.get('/udvozlet', (req, res) => {
    console.log("Udvozol a HTTP AMQP GATEWAY!");
    // Itt küldhetnéd tovább a RabbitMQ-nak!
    res.status(200).send({ message: "Udvozol a HTTP AMQP GATEWAY!" });
});

app.get('/keepalive', (req, res) => {
    console.log('This is a keep-alive transaction, to test the server connection.');
    res.status(200).send({ message: "Done." });
})

/**
 * POST
 * 
 * 1. Initialize RabbitMQ connection
 * 2. Send a Message
 * 3. Close the connection
 * 
 */
app.post('/uzenet', async (req, res) => {
    console.log("Adat érkezett:", req.body);

    try {

        sharedConnection = await amqp.connect(CLOUDAMQP_URL);
        sharedChannel = await sharedConnection.createChannel();

        await sendMessage({
            id: '1',
            topic: 'hibabejelento',
            message: req.body,
            timestamp: new Date().toISOString()
        });

        res.status(200).send({ status: "Siker!" });

        // Várjunk 100 milliszekundumot (ez általában elég)
        await new Promise(resolve => setTimeout(resolve, 100));

        if (sharedConnection) {
            console.log('Close AMQP connection.');
            await sharedConnection.close();
        }

    } catch (error) {
        console.error("🚨 KRITIKUS HIBA: Nem sikerült csatlakozni a RabbitMQ-hoz!", error.message);
        return; // Ha a kapcsolat hibás, állítsuk le a játékot
    }

});

app.listen(3000, () => console.log("Szerver fut a 3000-es porton"));