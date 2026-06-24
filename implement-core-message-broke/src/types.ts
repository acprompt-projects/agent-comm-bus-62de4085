export interface AgentInfo {
  id: string;
  channels: Set<string>;
  connectedAt: number;
  lastSeen: number;
  metadata?: Record<string, unknown>;
}

export interface BusMessage {
  id: string;
  type: "publish" | "subscribe" | "unsubscribe" | "ping";
  channel: string;
  payload?: unknown;
  senderId: string;
  timestamp: number;
}

export interface OutboundMessage {
  type: "message" | "subscribed" | "unsubscribed" | "pong" | "error" | "presence";
  channel: string;
  payload?: unknown;
  senderId?: string;
  timestamp: number;
}

export interface HealthStatus {
  status: "ok" | "shutting_down";
  uptime: number;
  agents: number;
  channels: number;
  timestamp: number;
}