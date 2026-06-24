import {
  BusConfig,
  ConnectionListener,
  ConnectionState,
  Handler,
  MessageEnvelope,
  PendingRequest,
  ReplyHandler,
} from "./types";

type WsLike = { send(data: string): void; close(): void; readyState: number };
const WS_OPEN = 1;

function makeId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
}

export class AgentBusClient {
  private config: Required<BusConfig>;
  private ws: WsLike | null = null;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private subs = new Map<string, Set<Handler>>();
  private replyHandlers = new Map<string, ReplyHandler>();
  private pendingRequests = new Map<string, PendingRequest>();
  private connectionListeners = new Set<ConnectionListener>();
  private state: ConnectionState = "disconnected";
  private intentionalClose = false;

  constructor(config: BusConfig) {
    this.config = {
      reconnectIntervalMs: 2000,
      maxReconnectAttempts: Infinity,
      requestTimeoutMs: 5000,
      ...config,
    };
  }

  connect(): void {
    if (this.ws) return;
    this.intentionalClose = false;
    this.setState("connecting");
    this.openSocket();
  }

  disconnect(): void {
    this.intentionalClose = true;
    this.clearReconnect();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    for (const [, p] of this.pendingRequests) {
      clearTimeout(p.timer);
      p.reject(new Error("Disconnected"));
    }
    this.pendingRequests.clear();
    this.setState("disconnected");
  }

  onConnectionChange(listener: ConnectionListener): () => void {
    this.connectionListeners.add(listener);
    listener(this.state);
    return () => this.connectionListeners.delete(listener);
  }

  publish<T>(channel: string, payload: T): void {
    this.sendEnvelope<T>({ type: "publish", channel, payload });
  }

  subscribe<T>(channel: string, handler: Handler<T>): () => void {
    let set = this.subs.get(channel);
    if (!set) {
      set = new Set();
      this.subs.set(channel, set);
      this.sendEnvelope({ type: "subscribe", channel, payload: null });
    }
    const typed = handler as Handler;
    set.add(typed);
    return () => {
      set!.delete(typed);
      if (set!.size === 0) {
        this.subs.delete(channel);
        this.sendEnvelope({ type: "unsubscribe", channel, payload: null });
      }
    };
  }

  request<TReq, TRes>(channel: string, payload: TReq, timeoutMs?: number): Promise<TRes> {
    const correlationId = makeId();
    return new Promise<TRes>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(new Error(`Request to "${channel}" timed out`));
      }, timeoutMs ?? this.config.requestTimeoutMs);
      this.pendingRequests.set(correlationId, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });
      this.sendEnvelope<TReq>({ type: "request", channel, payload, correlationId });
    });
  }

  reply<TReq, TRes>(channel: string, handler: ReplyHandler<TReq, TRes>): () => void {
    this.replyHandlers.set(channel, handler as ReplyHandler);
    this.sendEnvelope({ type: "subscribe", channel, payload: null });
    return () => {
      this.replyHandlers.delete(channel);
    };
  }

  private sendEnvelope<T>(partial: {
    type: MessageEnvelope["type"];
    channel: string;
    payload: T;
    correlationId?: string;
  }): void {
    const env: MessageEnvelope<T> = {
      id: makeId(),
      channel: partial.channel,
      type: partial.type,
      payload: partial.payload,
      correlationId: partial.correlationId,
      timestamp: Date.now(),
      sender: this.config.agentId,
    };
    if (this.ws && this.ws.readyState === WS_OPEN) {
      this.ws.send(JSON.stringify(env));
    }
  }

  private handleMessage(raw: string): void {
    const env: MessageEnvelope = JSON.parse(raw);

    if (env.type === "reply" && env.correlationId) {
      const pending = this.pendingRequests.get(env.correlationId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingRequests.delete(env.correlationId);
        pending.resolve(env.payload);
      }
      return;
    }

    if (env.type === "request" && env.correlationId) {
      const handler = this.replyHandlers.get(env.channel);
      if (handler) {
        Promise.resolve(handler(env.payload, env)).then((result) => {
          this.sendEnvelope({
            type: "reply",
            channel: env.channel,
            payload: result,
            correlationId: env.correlationId,
          });
        });
      }
      return;
    }

    if (env.type === "publish") {
      const handlers = this.subs.get(env.channel);
      if (handlers) {
        for (const h of handlers) {
          try { h(env.payload, env); } catch (_) { /* swallow handler errors */ }
        }
      }
    }
  }

  private openSocket(): void {
    const WsCtor = typeof WebSocket !== "undefined" ? WebSocket : require("ws");
    const ws = new WsCtor(this.config.url) as WsLike;

    ws.onopen = () => {
      this.reconnectAttempt = 0;
      this.setState("connected");
      this.resubscribeAll();
    };

    ws.onmessage = (ev: { data: string }) => this.handleMessage(ev.data);

    ws.onclose = () => {
      this.ws = null;
      if (!this.intentionalClose) this.scheduleReconnect();
    };

    ws.onerror = () => { /* close handler covers reconnection */ };

    this.ws = ws;
  }

  private resubscribeAll(): void {
    for (const ch of this.subs.keys()) {
      this.sendEnvelope({ type: "subscribe", channel: ch, payload: null });
    }
    for (const ch of this.replyHandlers.keys()) {
      this.sendEnvelope({ type: "subscribe", channel: ch, payload: null });
    }
  }

  private scheduleReconnect(): void {
    if (this.intentionalClose) return;
    if (this.reconnectAttempt >= this.config.maxReconnectAttempts) {
      this.setState("disconnected");
      return;
    }
    this.setState("reconnecting");
    this.reconnectAttempt++;
    const delay = this.config.reconnectIntervalMs * Math.min(this.reconnectAttempt, 10);
    this.reconnectTimer = setTimeout(() => this.openSocket(), delay);
  }

  private clearReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.reconnectAttempt = 0;
  }

  private setState(s: ConnectionState): void {
    this.state = s;
    for (const l of this.connectionListeners) l(s);
  }
}