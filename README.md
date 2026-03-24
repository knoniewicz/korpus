# Korpus

Korpus is a Go service that owns the canonical write path for your data: it validates events against JSON Schemas, writes them to Postgres, and keeps persistence logic in one place.

## What Korpus is

Korpus is the persistence boundary for systems that publish structured events but do not want every service to reinvent database writes, model handling, validation, retries, and foreign-key ordering.

Instead of letting many services write directly to Postgres, they publish events. Korpus validates those events, resolves write order, and persists the canonical record.

This keeps contracts explicit and makes writes predictable.

## Why it exists

Most multi-service systems eventually duplicate the same work in too many places:

- mapping payloads into tables
- validating input shapes
- handling upserts
- retrying writes when dependencies arrive late
- keeping schemas and database contracts in sync

Korpus centralizes that work.

You define schemas once, publish events, and let Korpus handle the durable write path.

## Core responsibilities

- Load JSON Schemas from disk
- Validate incoming event payloads before writing
- Create and prepare Postgres tables/statements from schema definitions
- Upsert canonical records into Postgres
- Handle foreign-key and dependency ordering
- Retry writes when parent records are not available yet
- Expose health and read/query endpoints
- Emit structured logs for operations and failures

## How it works

At a high level:

1. A producer publishes an event to Redis.
2. Korpus receives the event.
3. The payload is validated against the matching JSON Schema.
4. The event is queued for writing.
5. Korpus writes the record to Postgres, including FK-aware retries when needed.
6. Downstream consumers can query the canonical data over HTTP.

## Write flow

```text
Redis publish
  -> Korpus event handler
  -> JSON Schema validation
  -> write queue
  -> FK/dependency-aware upsert into Postgres
  -> optional follow-up notifications / reads
```

## Schema ownership and layout

Korpus is designed so **extensions own their own schemas**.

Primary (recommended) layout:

```text
./extensions/
  <extension>/
    schemas/
      <entity>.json
```

Example:

```text
./extensions/
  billing/
    schemas/
      invoice.json
  identity/
    schemas/
      user.json
```

Korpus also supports a flat service layout when needed:

```text
./schemas/
  <service>/
    <entity>.json
```

Each schema becomes the basis for validation, table generation, and query metadata.

## Configuration

| Variable | Default | Required | Description |
|---|---|---:|---|
| `DATABASE_URL` | none | yes* | Full Postgres connection string |
| `POSTGRES_USER` | none | yes* | Used if `DATABASE_URL` is not set |
| `POSTGRES_PASSWORD` | none | yes* | Used if `DATABASE_URL` is not set |
| `POSTGRES_HOST` | none | yes* | Used if `DATABASE_URL` is not set |
| `POSTGRES_PORT` | none | yes* | Used if `DATABASE_URL` is not set |
| `POSTGRES_DB` | none | yes* | Used if `DATABASE_URL` is not set |
| `POSTGRES_SSLMODE` | `disable` | no | SSL mode applied when building a URL from `POSTGRES_*`, or when `DATABASE_URL` omits `sslmode` |
| `REDIS_ADDR` | `127.0.0.1:6379` | no | Redis address |
| `HTTP_PORT` | `4222` | no | HTTP server port |
| `SCHEMA_DIR` | `./extensions` | no | Root directory for schema discovery |
| `MAX_WORKERS` | CPU count | no | Worker count for processing events |
| `REDIS_BUFFER_SIZE` | `10000` | no | Redis/event buffer size |
| `AUTH_TOKEN` | none | no | Required for authenticated HTTP endpoints |

`*` Either set `DATABASE_URL` or provide the individual `POSTGRES_*` variables.

## Schema discovery patterns

Korpus supports exactly these schema path patterns under `SCHEMA_DIR`:

- `<root>/<extension>/schemas/<entity>.json` (primary)
- `<root>/<service>/<entity>.json` (optional fallback)

The primary pattern is recommended for extension-owned schemas.

## Primary key behavior

- If a schema field has `x-primary-key: true`, that field is used as the table primary key.
- If no field is marked with `x-primary-key: true`, Korpus injects an `id` primary key automatically.

You should not define `id` manually in schemas.

## Docker and schema mounting

If you run Korpus in Docker, mount your extensions directory (or schema root) into the container and point `SCHEMA_DIR` at that mounted path.

Example:

```bash
docker run --rm \
  -p 4222:4222 \
  -e DATABASE_URL="postgres://postgres:postgres@host.docker.internal:5432/korpus?sslmode=disable" \
  -e REDIS_ADDR="host.docker.internal:6379" \
  -e HTTP_PORT="4222" \
  -e SCHEMA_DIR="/app/extensions" \
  -v "$(pwd)/extensions:/app/extensions:ro" \
  korpus:latest
```

The important part is that schema loading is not tied to `../../extensions` or any hardcoded parent-repo layout. Point `SCHEMA_DIR` to the root you mount.

## Running locally

```bash
go run .
```

Before starting Korpus, make sure Postgres and Redis are reachable and your schema directory exists.

If you are running from this source tree, start the service from the repository root you intend to use and set `SCHEMA_DIR` explicitly.

Example:

```bash
SCHEMA_DIR=./extensions HTTP_PORT=4222 go run .
```

For a standalone schema-only layout:

```bash
SCHEMA_DIR=./schemas HTTP_PORT=4222 go run .
```

Want a copy-paste setup with Postgres + Redis + sample schema?

See [`examples/`](./examples).

For a standalone local stack in this repo:

```bash
docker compose up --build
```

Use [`.env.example`](./.env.example) as your starting environment file.

You can also use the included `Makefile`:

```bash
make run
make test
make docker-up
```

## HTTP API

### `GET /health`

Returns service health, including Redis and Postgres checks.

### `GET /schemas`

Lists loaded schemas.

Query params:

- `service`
- `entity`

### `GET /entities`

Queries stored entities.

Query params include:

- `service`
- `entity`
- `limit`
- `offset`
- `sort`
- `fields`
- field filters like `name=value`, `created_at__gte=...`, `status__in=a,b`
- `resolve`
- `depth`
- `include_children`

### `GET /events`

Streams server-sent events.

### `POST /events/publish`

Publishes a write event through the HTTP API.

Example body:

```json
{
  "service": "billing",
  "entity": "invoice",
  "action": "created",
  "payload": {
    "external_id": "inv_123"
  }
}
```

Valid actions:

- `created`
- `ended`
- `deleted`

## Authentication

Most data/query endpoints require `AUTH_TOKEN` to be configured and sent as the `x-korpus-token` header.

`GET /health` does not require authentication.

## Troubleshooting

### Schema directory not found

Make sure `SCHEMA_DIR` points to a real directory inside the current runtime environment. In Docker, that usually means checking both the volume mount and the in-container path.

### Validation failures

The payload does not match the entity schema. Check required fields, types, enums, and nullable fields.

### Repeated dependency retries

A child record is arriving before its parent, or the schema dependency graph does not match the actual event order.

### Health endpoint is degraded

Korpus can start while dependencies are unhealthy, but `/health` will report Redis or Postgres failures clearly. Check connection settings first.

## Design notes

- Korpus is intentionally narrow in scope.
- It is the write layer, not the full application.
- It works best when other services treat it as the single durable persistence path.
- Keeping schemas close to the write path makes contracts easier to reason about and easier to change safely.

## Contributing

If you want to contribute, the most useful improvements are usually concrete ones: better schema handling, clearer observability, tighter retry behavior, stronger tests, or cleaner operational docs.

Small, focused pull requests are appreciated.

## License

Korpus is available under **AGPLv3**.

If you want to use Korpus in a proprietary or commercial setting without AGPL obligations, a separate commercial license is available.

See [`LICENSE`](./LICENSE) and [`COMMERCIAL_LICENSE.md`](./COMMERCIAL_LICENSE.md).
