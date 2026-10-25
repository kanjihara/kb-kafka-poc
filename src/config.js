module.exports = {
  port: Number(process.env.PORT || 3000),
  apiKey: process.env.API_KEY || 'change-me',
  public: {
    brokers: (process.env.PUBLIC_BROKERS || 'localhost:9092').split(','),
    topic: process.env.PUBLIC_TOPIC || 'public-topic',
  },
  secure: {
    brokers: (process.env.SECURE_BROKERS || 'localhost:9094').split(','),
    topic: process.env.SECURE_TOPIC || 'secure-topic',
    username: process.env.SECURE_USERNAME || 'app',
    password: process.env.SECURE_PASSWORD || 'app-secret',
  },
};
