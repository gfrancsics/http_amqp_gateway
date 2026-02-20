const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

const app = express();

const CLOUDAMQP_URL = 'amqps://vpwdmwwi:5WAHg6cVzwB0duCEeznFHl_W0eqfyF27@cow.rmq2.cloudamqp.com/vpwdmwwi';
const EXCHANGE_NAME = 'email';
const QUEUE_NAME = 'hibajelentesek_sora';

let sharedConnection = null;
let sharedChannel = null;

/**
 * LASY INITIALISATION
 * RENDER sends our service to sleep after 15 minutes of inactivity.
 * Therefore we have to check the connection before every HTTP request.
 * If the connection is not ready we have to reinitialise but if it is working,
 * there is no need to do it again.
 */
async function getChannel() {
    // Ha már van élő kapcsolat és csatormna, egyszerűen adjuk vissza
    if (sharedConnection && sharedChannel) {
        return sharedChannel;
    }

    try {
        console.log("🔌 Új RabbitMQ kapcsolat felépítése (ébredés után)...");
        sharedConnection = await amqp.connect(CLOUDAMQP_URL);
        sharedChannel = await sharedConnection.createChannel();

        // Mindenképp deklaráljuk a struktúrát, biztos ami biztos
        await sharedChannel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
        await sharedChannel.assertQueue(QUEUE_NAME, { durable: true, autoDelete: false });
        await sharedChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');

        // Ha a kapcsolat hiba miatt szakad meg, nullázzuk a változókat
        sharedConnection.on("error", () => {
            console.error("AMQP hiba, változók nullázása...");
            sharedConnection = null; sharedChannel = null;
        });
        sharedConnection.on("close", () => {
            console.error("AMQP hiba, váratlan leállás...");
            sharedConnection = null; sharedChannel = null;
        });

        return sharedChannel;
    } catch (err) {
        console.error("❌ RabbitMQ hiba:", err);
        return null;
    }
}

/**
 * RENDER sends our service to sleep after 15 minutes.
 * We have to close the AMQP connection and logging out this event.
 */
const gracefulShutdown = async () => {
    console.log("⚠️ Render leállási jel érkezett. Kapcsolatok zárása...");
    try {
        if (channel) {
            await channel.close();
            console.log("✅ RabbitMQ csatorna lezárva.");
        }
        if (connection) {
            await connection.close();
            console.log("✅ RabbitMQ kapcsolat lezárva.");
        }
    } catch (err) {
        console.error("Hiba a leállás során:", err);
    } finally {
        process.exit(0);
    }
};

/**
 * 
 */
const createUniqeIDFromTimeStamp = function (timeStamp) {
    return timeStamp ? timeStamp.getTime().toString(36) + Math.random().toString(36).substring(2, 10) : null;
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
    const activeChannel = await getChannel();

    if (!activeChannel) {
        return res.status(500).json({ hiba: "Nem sikerült kapcsolódni az üzenetsorhoz" });
    }

    const uniqeID = createUniqeIDFromTimeStamp(new Date())

    const message = {
        id: uniqeID,
        topic: 'hibabejelento',
        message: Buffer.from(JSON.stringify(req.body)),
        timestamp: new Date().toISOString()
    };

    const sent = activeChannel.publish(
        EXCHANGE_NAME,
        '',
        message,
        { persistent: true }
    );

    if (sent) {
        res.status(202).json({ status: "Siker, az üzenet a sorban!" });
    } else {
        res.status(500).json({ hiba: "A sor megtelt vagy hiba történt" });
    }
});

app.listen(3000, () => console.log("Szerver fut a 3000-es porton"));

// Figyeljük a Render (vagy a Node) leállító jeleit
process.on('SIGTERM', gracefulShutdown); // A Render ezt küldi leálláskor
process.on('SIGINT', gracefulShutdown);  // Ez a Ctrl+C-re reagál (helyi teszteléskor)