const express = require('express');
const swaggerUi = require('swagger-ui-express');
const config = require('./config');
const kafka = require('./kafka');
const routes = require('./routes');
const openapi = require('./openapi');

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.get('/openapi.json', (req, res) => res.json(openapi));
app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapi));
app.use('/api', routes);

const server = app.listen(config.port, () => {
  console.log(`API on :${config.port} - Swagger at http://localhost:${config.port}/docs`);
});

const stop = () => server.close(() => kafka.shutdown().finally(() => process.exit(0)));
process.on('SIGTERM', stop);
process.on('SIGINT', stop);
