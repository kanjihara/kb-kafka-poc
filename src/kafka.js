const { Kafka, logLevel } = require('kafkajs');
const config = require('./config');

const clients = {
  public: new Kafka({
    clientId: 'kb-kafka-poc-public',
    brokers: config.public.brokers,
    logLevel: logLevel.WARN,
  }),
  secure: new Kafka({
    clientId: 'kb-kafka-poc-secure',
    brokers: config.secure.brokers,
    logLevel: logLevel.WARN,
    sasl: {
      mechanism: 'plain',
      username: config.secure.username,
      password: config.secure.password,
    },
  }),
};

const producers = {};

async function getProducer(name) {
  if (!producers[name]) {
    const producer = clients[name].producer();
    producers[name] = producer.connect().then(() => producer).catch((err) => {
      delete producers[name];
      throw err;
    });
  }
  return producers[name];
}

async function produce(name, { key, value, headers }) {
  const producer = await getProducer(name);
  const payload = typeof value === 'string' ? value : JSON.stringify(value);
  return producer.send({
    topic: config[name].topic,
    messages: [{ key: key ?? null, value: payload, headers }],
  });
}

// Reads up to `limit` messages, or stops after `timeoutMs`, using a short-lived consumer.
async function consume(name, { groupId, limit, timeoutMs, fromBeginning }) {
  const consumer = clients[name].consumer({ groupId });
  const messages = [];
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: config[name].topic, fromBeginning });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, timeoutMs);
      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            const raw = message.value ? message.value.toString() : null;
            let value = raw;
            try { value = JSON.parse(raw); } catch (_) { /* keep raw string */ }
            messages.push({
              partition,
              offset: message.offset,
              timestamp: message.timestamp,
              key: message.key ? message.key.toString() : null,
              value,
            });
            if (messages.length >= limit) {
              clearTimeout(timer);
              resolve();
            }
          },
        })
        .catch((err) => { clearTimeout(timer); reject(err); });
    });
  } finally {
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
  }
  return messages;
}

async function shutdown() {
  await Promise.all(
    Object.values(producers).map((p) => p.then((x) => x.disconnect()).catch(() => {}))
  );
}

module.exports = { produce, consume, shutdown };
