# Korpus Example

This folder gives you a minimal runnable setup for Korpus with extension-owned schemas.

## Layout

```text
examples/
  docker-compose.yml
  extensions/
    users/
      schemas/
        users.json
```

## Run

```bash
cd examples
docker compose up --build
```

Korpus will load schemas from:

```text
/app/examples/extensions
```

via `SCHEMA_DIR=/app/examples/extensions`.

## Quick checks

Health:

```bash
curl http://localhost:4222/health
```

List schemas (requires token header):

```bash
curl -H "x-korpus-token: dev-token" "http://localhost:4222/schemas"
```

Publish event:

```bash
curl -X POST "http://localhost:4222/events/publish" \
  -H "Content-Type: application/json" \
  -H "x-korpus-token: dev-token" \
  -d '{
    "service": "users",
    "entity": "users",
    "action": "created",
    "payload": {
      "external_id": "user_1",
      "name": "Ada Lovelace",
      "email": "ada@example.com"
    }
  }'
```

Query entities:

```bash
curl -H "x-korpus-token: dev-token" "http://localhost:4222/entities?service=users&entity=users"
```
