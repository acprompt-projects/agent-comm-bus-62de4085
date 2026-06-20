# Agent Comm Bus — Architecture & Protocol Design

## 1. Overview

Agent Comm Bus (ACB) is a lightweight, real-time message bus enabling ACPrompt agents to publish/subscribe to typed events and exchange structured data. It supports both pub-sub and point-to-point messaging over WebSocket (primary) and HTTP (fallback).

## 2. Transport Layer

| Transport | Use Case | Direction |
|-----------|----------|-----------|
| WebSocket (`ws://`) | Real-time streaming, subscriptions, live events | Bidirectional |
| HTTP (`http://`) | One-shot publish, agent registry, health checks | Request-Response |

- Default port: `6473`
- All payloads are UTF-8 JSON.
- WebSocket frames are text only (no binary frames).
- HTTP endpoints mirror WebSocket message types for compatibility.

## 3. Agent Addressing

Each agent is identified by a globally unique `agent_id`:

```
agent_id := <namespace>/<name>
namespace := [a-z0-9][a-z0-9-]{0,31}
name      := [a-z0-9][a-z0-9-]{0,63}
```

Examples: `core/orchestrator`, `tools/web-search`, `llm/gpt-4o`

Agents register with the bus on connect. The bus assigns a connection-bound `session_id` (UUIDv4) used internally for routing.

## 4. Channel & Topic Model

```
channel := <domain>/<subdomain>/<event-type>
```

- Segments are hierarchical; wildcards supported:
  - `*` matches one segment: `tools/*/result`
  - `#` matches zero or more trailing segments: `llm/#`
- Channels are case-sensitive, lowercase by convention.
- Maximum depth: 6 segments.

Examples:
- `task/created` — task lifecycle events
- `llm/gpt-4o/token-stream` — streaming token output
- `tools/web-search/result` — tool execution results

## 5. Message Format (Envelope)

All messages share a common envelope. The `type` field determines the payload schema.

```json
{
  "ver": 1,
  "id": "msg-uuidv4",
  "src": "core/orchestrator",
  "ts": "2025-01-15T10:30:00.000Z",
  "type": "event | cmd | ack | err | register | subscribe | unsubscribe",
  "chan": "task/created",
  "dst": null,
  "corr": null,
  "ttl": 30,
  "payload": { }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ver` | int | Protocol version (currently `1`) |
| `id` | string (UUID) | Unique message identifier |
| `src` | string | Sender `agent_id` |
| `ts` | string (ISO 8601) | Timestamp of origination |
| `type` | enum | Message type (see §6) |
| `chan` | string | Channel/topic |
| `dst` | string\|null | Target `agent_id` for point-to-point; `null` for broadcast |
| `corr` | string\|null | Correlation ID; echoes `id` of request in response |
| `ttl` | int | Time-to-live in seconds; `0` = no expiry |
| `payload` | object | Type-specific payload (see §6) |

## 6. Message Types & Payloads

### 6.1 `register` — Agent announces itself
```json
{
  "type": "register",
  "chan": null,
  "payload": {
    "capabilities": ["task.create", "llm.invoke"],
    "metadata": { "version": "1.2.0" }
  }
}
```

### 6.2 `subscribe` / `unsubscribe` — Channel subscription
```json
{
  "type": "subscribe",
  "chan": "task/#",
  "payload": null
}
```

### 6.3 `event` — Pub-sub broadcast
```json
{
  "type": "event",
  "chan": "task/created",
  "dst": null,
  "payload": {
    "task_id": "t-abc123",
    "description": "Summarize article"
  }
}
```

### 6.4 `cmd` — Point-to-point command (request)
```json
{
  "type": "cmd",
  "chan": "llm/gpt-4o/invoke",
  "dst": "llm/gpt-4o",
  "corr": null,
  "payload": {
    "prompt": "Hello",
    "max_tokens": 256
  }
}
```

### 6.5 `ack` — Acknowledgment / response
```json
{
  "type": "ack",
  "chan": "llm/gpt-4o/invoke",
  "dst": "core/orchestrator",
  "corr": "msg-uuid-of-cmd",
  "payload": {
    "status": "ok",
    "data": { "response": "Hi there!" }
  }
}
```

### 6.6 `err` — Error response
```json
{
  "type": "err",
  "chan": "llm/gpt-4o/invoke",
  "dst": "core/orchestrator",
  "corr": "msg-uuid-of-cmd",
  "payload": {
    "code": "RATE_LIMITED",
    "message": "Too many requests"
  }
}
```

## 7. Messaging Patterns

### 7.1 Publish-Subscribe
1. Agent A sends `subscribe` on channel pattern.
2. Agent B publishes `event` to channel.
3. Bus matches subscriptions and delivers `event` to all matching subscribers.

### 7.2 Point-to-Point (Request-Reply)
1. Agent A sends `cmd` with `dst` set to Agent B's `agent_id`.
2. Bus routes to Agent B only.
3. Agent B responds with `ack` or `err`, setting `corr` to original `cmd.id`.
4. Agent A correlates response.

### 7.3 Fan-Out with Ack
1. Agent A publishes `cmd` with `dst: null` on a channel.
2. Bus delivers to all subscribers.
3. Each subscriber may reply with `ack`/`err` using `corr`.
4. Agent A collects responses.

## 8. WebSocket Protocol Sequence

```
┌──────────┐                         ┌──────────┐
│  Agent A  │                         │   Bus    │
└─────┬─────┘                         └────┬─────┘
      │  WS Connect                        │
      │──────────────────────────────────►│
      │  { type: "register", ... }         │
      │──────────────────────────────────►│
      │  { type: "ack", corr: ... }        │
      │◄──────────────────────────────────│
      │  { type: "subscribe", chan: .. }   │
      │──────────────────────────────────►│
      │  { type: "ack", corr: ... }        │
      │◄──────────────────────────────────│
      │                                    │
      │          ... time passes ...       │
      │                                    │
      │  { type: "event", chan: ... }      │  (from another agent)
      │◄──────────────────────────────────│
      │                                    │
```

### Point-to-Point Sequence

```
┌──────────┐                  ┌──────────┐              ┌──────────┐
│  Agent A  │                  │   Bus    │              │  Agent B  │
└─────┬─────┘                  └────┬─────┘              └─────┬─────┘
      │  { type:"cmd", dst:B }    │                          │
      │─────────────────────────►│  { type:"cmd", dst:B }   │
      │                          │─────────────────────────►│
      │                          │                          │
      │                          │  { type:"ack", corr:id } │
      │  { type:"ack", corr:id } │◄─────────────────────────│
      │◄─────────────────────────│                          │
```

## 9. HTTP API Contracts

Base URL: `http://host:6473/api/v1`

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/publish` | Publish a message (body = envelope with `type:event\|cmd`) |
| `POST` | `/subscribe` | Register an HTTP callback URL for a channel pattern |
| `DELETE` | `/subscribe/:sub_id` | Remove an HTTP subscription |
| `GET` | `/agents` | List connected agents |
| `GET` | `/channels` | List active channels & subscriber counts |
| `GET` | `/health` | Health check → `{ "status": "ok" }` |

### POST /publish
```json
// Request body is a message envelope
{ "ver":1, "id":"...", "src":"tools/x", "ts":"...", "type":"event",
  "chan":"tools/x/result", "dst":null, "corr":null, "ttl":30,
  "payload": { "value": 42 } }

// Response
{ "delivered": 3 }
```

### POST /subscribe
```json
// Request
{ "agent_id": "tools/x", "chan": "task/#", "callback": "https://tools-x:8080/events" }

// Response
{ "sub_id": "sub-uuid" }
```

## 10. Error Codes

| Code | Meaning |
|------|---------|
| `UNKNOWN_AGENT` | `dst` agent not connected |
| `NO_SUBSCRIBERS` | Channel has no subscribers (info, not error) |
| `RATE_LIMITED` | Agent exceeded rate limit |
| `INVALID_MESSAGE` | Malformed envelope or payload |
| `CHANNEL_DENIED` | Agent not authorized for channel |
| `TTL_EXPIRED` | Message exceeded time-to-live |

## 11. Reliability & Ordering

- **At-most-once** delivery by default (fire and forget).
- **At-least-once** optional: set `ttl > 0` + `corr`; sender retries until `ack` or TTL expiry.
- Messages on the same channel from the same source are delivered in order per subscriber.
- No guaranteed cross-channel ordering.

## 12. Security Considerations

- TLS mandatory in production (`wss://`, `https://`).
- Agent authentication via JWT in WebSocket upgrade header or HTTP `Authorization: Bearer <token>`.
- Channel-level ACLs: agents declare capabilities on `register`; bus enforces publish/subscribe permissions.
- Rate limiting per `agent_id`: configurable tokens/second bucket.