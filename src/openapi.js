const messageBody = {
  required: true,
  content: {
    'application/json': {
      schema: {
        type: 'object',
        required: ['value'],
        properties: {
          key: { type: 'string', example: 'order-1' },
          value: { description: 'String or any JSON', example: { hello: 'world' } },
          headers: { type: 'object', additionalProperties: { type: 'string' } },
        },
      },
    },
  },
};

const consumeParams = [
  { name: 'groupId', in: 'query', schema: { type: 'string' }, description: 'Consumer group (offsets are committed per group)' },
  { name: 'limit', in: 'query', schema: { type: 'integer', default: 10, maximum: 500 } },
  { name: 'timeoutMs', in: 'query', schema: { type: 'integer', default: 10000, maximum: 60000 }, description: 'Max wait, includes consumer group join time' },
  { name: 'fromBeginning', in: 'query', schema: { type: 'boolean', default: true }, description: 'Only applies to a group with no committed offsets' },
];

function pathsFor(tag, secured) {
  const security = secured ? [{ ApiKeyAuth: [] }] : [];
  const errors = secured
    ? { 401: { description: 'Missing/invalid x-api-key or Kafka SASL credentials' }, 403: { description: 'Kafka ACL denied access to the topic' } }
    : {};
  return {
    post: {
      tags: [tag], summary: `Produce a message to the ${tag} topic`, security,
      requestBody: messageBody,
      responses: { 201: { description: 'Message produced' }, 400: { description: 'Invalid body' }, ...errors },
    },
    get: {
      tags: [tag], summary: `Consume messages from the ${tag} topic`, security,
      parameters: consumeParams,
      responses: { 200: { description: 'Messages read' }, ...errors },
    },
  };
}

module.exports = {
  openapi: '3.0.3',
  info: {
    title: 'Kafka POC API',
    version: '1.0.0',
    description: 'Produce and consume Kafka messages. `public` uses an unauthenticated listener; `secure` uses SASL/PLAIN + ACLs and also requires the `x-api-key` header.',
  },
  paths: {
    '/api/public/messages': pathsFor('public', false),
    '/api/secure/messages': pathsFor('secure', true),
    '/health': { get: { tags: ['health'], summary: 'Health check', responses: { 200: { description: 'OK' } } } },
  },
  components: {
    securitySchemes: { ApiKeyAuth: { type: 'apiKey', in: 'header', name: 'x-api-key' } },
  },
};
