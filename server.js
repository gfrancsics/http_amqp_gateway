const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

const app = express();

const CLOUDAMQP_URL = 'amqps://efrhhyke:pyP8fjOBbh022nGhZp5GQDHoCoaUEEO9@cow.rmq2.cloudamqp.com/efrhhyke';
const EXCHANGE_NAME = 'game_logs';
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

        // 1. Exchange deklarálása a Producer oldalon is (fanout típus)
        await sharedChannel.assertExchange(EXCHANGE_NAME, 'fanout', {
            durable: true
        });

        // 2. Publikálás a fanout Exchange-re
        // A routingKey-nek üres stringnek kell lennie (''), 
        // mivel a fanout Exchange ignorálja.
        const sent = await sharedChannel.publish(
            EXCHANGE_NAME, // A Exchange, amire publikálunk
            '',            // Routing Key: Üresen hagyjuk
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