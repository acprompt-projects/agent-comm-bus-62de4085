import { createServer, IncomingMessage, ServerResponse, Server } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { v4 as uuid } from "crypto";
import { AgentInfo, BusMessage, OutboundMessage, HealthStatus } from "./types";

type RawUUID = string;
const newId = (): RawUUID => {
  const bytes = new Uint8Array(16);
  require("crypto").randomFillSync(bytes);
  return Array.from(bytes, (b: number) => b.toString(16).padStart(2, "0")).join("");
};

export class MessageBroker {
  private server: Server;
  private wss: WebSocketServer;
  private agents = new Map<WebSocket, AgentInfo>();
  private channels = new Map<string, Set<WebSocket>>();
  private startTime = Date.now();
  private shuttingDown = false;

  constructor(port: number = 9200) {
    this.server = createServer((req, res) => this.handleHttp(req, res));
    this.wss = new WebSocketServer({ server: this.server });

    this.wss.on("connection", (ws) => this.handleConnection(ws));
    this.wss.on("close", () => { /* cleaned up via server.close */ });

    process.on("SIGINT", () => this.shutdown());
    process.on("SIGTERM", () => this.shutdown());

    this.server.listen(port, () => {
      console.log(`[broker] listening on port ${port}`);
    });
  }

  /* ── WebSocket handling ─────────────────────────────── */

  private handleConnection(ws: WebSocket): void {
    const agentId = newId();
    const info: AgentInfo = {
      id: agentId,
      channels: new Set(),
      connectedAt: Date.now(),
      lastSeen: Date.now(),
    };
    this.agents.set(ws, info);
    this.send(ws, { type: "subscribed", channel: "__system__", payload: { agentId }, timestamp: Date.now() });
    ws.on("message", (raw) => this.handleMessage(ws, raw));
    ws.on("close", () => this.deregister(ws));
    ws.on("pong", () => { const a = this.agents.get(ws); if (a) a.lastSeen = Date.now(); });
  }

  private handleMessage(ws: WebSocket, raw: unknown): void {
    const agent = this.agents.get(ws);
    if (!agent) return;
    agent.lastSeen = Date.now();

    let msg: BusMessage;
    try {
      msg = JSON.parse(String(raw)) as BusMessage;
    } catch {
      this.send(ws, { type: "error", channel: "", payload: "invalid json", timestamp: Date.now() });
      return;
    }

    switch (msg.type) {
      case "subscribe":
        this.subscribe(ws, msg.channel);
        break;
      case "unsubscribe":
        this.unsubscribe(ws, msg.channel);
        break;
      case "publish":
        this.publish(ws, msg);
        break;
      case "ping":
        this.send(ws, { type: "pong", channel: "__ping__", timestamp: Date.now() });
        break;
      default:
        this.send(ws, { type: "error", channel: msg.channel, payload: `unknown type: ${msg.type}`, timestamp: Date.now() });
    }
  }

  /* ── Channel management ─────────────────────────────── */

  private subscribe(ws: WebSocket, channel: string): void {
    const agent = this.agents.get(ws);
    if (!agent) return;
    if (!this.channels.has(channel)) this.channels.set(channel, new Set());
    this.channels.get(channel)!.add(ws);
    agent.channels.add(channel);
    this.send(ws, { type: "subscribed", channel, timestamp: Date.now() });
    this.broadcastPresence(channel);
  }

  private unsubscribe(ws: WebSocket, channel: string): void {
    const agent = this.agents.get(ws);
    if (!agent) return;
    const subs = this.channels.get(channel);
    if (subs) {
      subs.delete(ws);
      if (subs.size === 0) this.channels.delete(channel);
    }
    agent.channels.delete(channel);
    this.send(ws, { type: "unsubscribed", channel, timestamp: Date.now() });
    this.broadcastPresence(channel);
  }

  private publish(ws: WebSocket, msg: BusMessage): void {
    const agent = this.agents.get(ws);
    if (!agent) return;
    const subs = this.channels.get(msg.channel);
    if (!subs) return;
    const out: OutboundMessage = {
      type: "message",
      channel: msg.channel,
      payload: msg.payload,
      senderId: agent.id,
      timestamp: Date.now(),
    };
    for (const sub of subs) {
      if (sub !== ws && sub.readyState === WebSocket.OPEN) {
        this.send(sub, out);
      }
    }
  }

  /* ── Presence ───────────────────────────────────────── */

  private broadcastPresence(channel: string): void {
    const subs = this.channels.get(channel);
    if (!subs) return;
    const members: string[] = [];
    for (const ws of subs) {
      const a = this.agents.get(ws);
      if (a) members.push(a.id);
    }
    const out: OutboundMessage = { type: "presence", channel, payload: { members }, timestamp: Date.now() };
    for (const ws of subs) {
      if (ws.readyState === WebSocket.OPEN) this.send(ws, out);
    }
  }

  /* ── Deregistration ─────────────────────────────────── */

  private deregister(ws: WebSocket): void {
    const agent = this.agents.get(ws);
    if (!agent) return;
    for (const ch of agent.channels) {
      const subs = this.channels.get(ch);
      if (subs) {
        subs.delete(ws);
        if (subs.size === 0) this.channels.delete(ch);
        else this.broadcastPresence(ch);
      }
    }
    this.agents.delete(ws);
    console.log(`[broker] agent ${agent.id} disconnected`);
  }

  /* ── HTTP / health ──────────────────────────────────── */

  private handleHttp(req: IncomingMessage, res: ServerResponse): void {
    if (req.url === "/health" && req.method === "GET") {
      const status: HealthStatus = {
        status: this.shuttingDown ? "shutting_down" : "ok",
        uptime: Date.now() - this.startTime,
        agents: this.agents.size,
        channels: this.channels.size,
        timestamp: Date.now(),
      };
      res.writeHead(this.shuttingDown ? 503 : 200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(status));
      return;
    }
    res.writeHead(404).end();
  }

  /* ── Graceful shutdown ──────────────────────────────── */

  public async shutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    console.log("[broker] shutting down…");

    const out: OutboundMessage = { type: "error", channel: "__system__", payload: "server shutting down", timestamp: Date.now() };
    for (const [ws] of this.agents) {
      this.send(ws, out);
      ws.close(1001, "shutting down");
    }

    this.wss.close();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    console.log("[broker] stopped");
  }

  /* ── Helpers ────────────────────────────────────────── */

  private send(ws: WebSocket, msg: OutboundMessage): void {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
  }

  public get health(): HealthStatus {
    return {
      status: this.shuttingDown ? "shutting_down" : "ok",
      uptime: Date.now() - this.startTime,
      agents: this.agents.size,
      channels: this.channels.size,
      timestamp: Date.now(),
    };
  }
}

export default MessageBroker;

if (require.main === module) {
  const port = parseInt(process.env.BROKER_PORT || "9200", 10);
  new MessageBroker(port);
}