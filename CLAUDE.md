# CLAUDE.md

Node.js REST API (Express + kafkajs) that produces/consumes two Kafka topics, with Kafka (KRaft) and AKHQ in Docker. See [context.md](context.md) for the full overview.

## Commands
- Start everything: `docker compose up -d --build`
- Full reset (needed after changing passwords, users or ACLs): `docker compose down -v && docker compose up -d --build`
- Logs: `docker compose logs -f api` (or `kafka`, `kafka-init`, `akhq`)
- Swagger UI: http://localhost:3000/docs, AKHQ: http://localhost:8080
- No test suite or linter exists. Verify changes with curl against the running stack.

## Layout
- `src/index.js` app setup; `src/routes.js` endpoints, API key check, error mapping; `src/kafka.js` kafkajs clients, producer, one-shot consumer; `src/openapi.js` Swagger spec (a JS object, not a file); `src/config.js` env settings
- `docker-compose.yml` Kafka, AKHQ and API; `kafka/init-topics.sh` creates topics and ACLs

## Rules and gotchas
- Two listeners: `kafka:9092` PLAINTEXT (`public-topic`) and `kafka:9094` SASL/PLAIN (`secure-topic`). Kafka is not published to the host; everything connects inside the Docker network.
- Authorization relies on `allow.everyone.if.no.acl.found=true`. Never add a wildcard (`*`) ACL on groups or the public topic: it locks anonymous clients out of the public topic and consumer groups. The group ACL is the prefix `secure-`.
- `/api/secure/*` routes force the `secure-` prefix on `groupId`. Keep that if you touch the consume route.
- Adding an API route means updating both `src/routes.js` and `src/openapi.js`, so Swagger stays accurate.
- ACLs and SASL users are set only at first start. Changes to them require the full reset above.
- The controller listener uses SASL on purpose; do not switch it to PLAINTEXT, since the broker would then appear as an anonymous user to the authorizer.
- `.env` holds local passwords and is gitignored. Keep `.env.example` in sync when adding variables, and keep the same defaults in `docker-compose.yml`.
- After changing the compose file, the init script or the API, re-test: produce and consume on both topics, and confirm an anonymous producer on `localhost:9092` inside the `kafka` container is rejected for `secure-topic`.
