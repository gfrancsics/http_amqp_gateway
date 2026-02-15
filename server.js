const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors()); // Ez engedélyezi a CORS-t mindenki számára
app.use(express.json());

app.post('/uzenet', (req, res) => {
    console.log("Adat érkezett:", req.body);
    // Itt küldhetnéd tovább a RabbitMQ-nak!
    res.status(200).send({ status: "Siker!" });
});

app.listen(3000, () => console.log("Szerver fut a 3000-es porton"));