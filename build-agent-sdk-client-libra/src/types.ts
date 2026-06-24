export interface MessageEnvelope<T = unknown> {
  id: string;
  channel: string;
  type: "publish" | "subscribe" | "request" | "reply";
  payload: T;
  correlationId?: string;
  timestamp: number;
  sender: string;
}

export interface BusConfig {
  url: string;
  agentId: string;
  reconnectIntervalMs?: number;
  maxReconnectAttempts?: number;
  requestTimeoutMs?: number;
}

export type Handler<T = unknown> = (payload: T, envelope: MessageEnvelope<T>) => void;
export type ReplyHandler<TReq = unknown, TRes = unknown> = (
  payload: TReq,
  envelope: MessageEnvelope<TReq>
) => Promise<TRes> | TRes;

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting";
export type ConnectionListener = (state: ConnectionState) => void;

export interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}