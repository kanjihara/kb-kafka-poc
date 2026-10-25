# kb-kafka-poc

Node.js REST API that produces/consumes Kafka topics, running with Docker alongside Kafka (KRaft) and AKHQ.

## Run

```bash
cp .env.example .env   # optional, defaults are the same values
docker compose up -d --build
```

| Service | URL |
|---|---|
| Swagger UI | http://localhost:3000/docs |
| AKHQ | http://localhost:8080 (connections `kafka-public` and `kafka-secure`) |

## Topics

| Topic | Listener | Security |
|---|---|---|
| `public-topic` | `kafka:9092` PLAINTEXT | none |
| `secure-topic` | `kafka:9094` SASL_PLAINTEXT (PLAIN) | authentication + ACLs |

Kafka users: `admin` (super user, used by AKHQ), `app` (read/write `secure-topic`, used by the API),
`reader` (read only). Anonymous clients on the plain listener are denied on `secure-topic`.
ACLs are created by [kafka/init-topics.sh](kafka/init-topics.sh).
Consumer groups prefixed `secure-` are restricted to `app`/`reader`.

## API

- `POST /api/public/messages` / `GET /api/public/messages`
- `POST /api/secure/messages` / `GET /api/secure/messages` (also needs header `x-api-key`, default `change-me`)

```bash
curl -XPOST localhost:3000/api/secure/messages -H 'Content-Type: application/json' \
  -H 'x-api-key: change-me' -d '{"key":"k1","value":{"hello":"world"}}'
curl "localhost:3000/api/secure/messages?limit=5" -H 'x-api-key: change-me'
```

`GET` opens a short-lived consumer, returns up to `limit` messages or whatever arrived within `timeoutMs`,
and commits offsets for the `groupId` (so repeated calls continue where the last one stopped).

Note: PLAIN over SASL_PLAINTEXT is not encrypted; use SASL_SSL (or SCRAM) outside local development.
