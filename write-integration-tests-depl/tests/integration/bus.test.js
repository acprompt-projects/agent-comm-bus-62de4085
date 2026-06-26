const { AgentBus } = require('../../src/agent-bus');
const { AuthProvider } = require('../../src/auth-provider');

const BUS_URL = process.env.BUS_URL || 'ws://localhost:8080';
const VALID_TOKEN = process.env.VALID_TOKEN || 'test-secret-token';
const INVALID_TOKEN = 'bad-token-12345';

describe('AgentBus Integration', () => {
  let bus;

  afterEach(async () => {
    if (bus) {
      try { await bus.disconnect(); } catch (_) {}
      bus = null;
    }
  });

  describe('Agent Registration', () => {
    test('registers an agent and receives agentId', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      const agent = await bus.connect({ agentType: 'test-worker', capabilities: ['parse'] });
      expect(agent.agentId).toBeDefined();
      expect(agent.agentId).toMatch(/^agent-/);
      expect(agent.connected).toBe(true);
    });

    test('rejects duplicate agentId on re-register', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      const agent = await bus.connect({ agentType: 'dup-worker' });
      const bus2 = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await expect(
        bus2.connect({ agentType: 'dup-worker', agentId: agent.agentId })
      ).rejects.toThrow(/already registered/i);
      await bus2.disconnect();
    });
  });

  describe('Auth Rejection', () => {
    test('rejects connection with invalid token', async () => {
      bus = new AgentBus({ url: BUS_URL, token: INVALID_TOKEN });
      await expect(bus.connect({ agentType: 'rogue' })).rejects.toThrow(/auth/i);
    });

    test('emits auth-failed event on bad credentials', async () => {
      bus = new AgentBus({ url: BUS_URL, token: INVALID_TOKEN });
      const failPromise = new Promise(resolve => bus.once('auth-failed', resolve));
      bus.connect({ agentType: 'rogue' }).catch(() => {});
      const evt = await failPromise;
      expect(evt.reason).toMatch(/invalid.*token|unauthorized/i);
    });
  });

  describe('Pub/Sub Across Channels', () => {
    test('subscriber receives published message on same channel', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await bus.connect({ agentType: 'pubsub-worker' });

      const received = new Promise(resolve => {
        bus.subscribe('channel:data-ingest', (msg) => resolve(msg));
      });

      await bus.publish('channel:data-ingest', { source: 's3', records: 42 });

      const msg = await received;
      expect(msg.source).toBe('s3');
      expect(msg.records).toBe(42);
    });

    test('subscriber does NOT receive messages from other channels', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await bus.connect({ agentType: 'isolation-worker' });

      let received = false;
      bus.subscribe('channel:alpha', () => { received = true; });
      await bus.publish('channel:beta', { text: 'wrong channel' });

      await new Promise(r => setTimeout(r, 300));
      expect(received).toBe(false);
    });

    test('multiple subscribers on same channel all receive message', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await bus.connect({ agentType: 'multi-sub' });

      const counts = { a: 0, b: 0 };
      bus.subscribe('channel:broadcast', () => { counts.a++; });
      bus.subscribe('channel:broadcast', () => { counts.b++; });

      await bus.publish('channel:broadcast', { event: 'tick' });
      await new Promise(r => setTimeout(r, 200));

      expect(counts.a).toBe(1);
      expect(counts.b).toBe(1);
    });
  });

  describe('Request-Reply', () => {
    test('requestor receives reply from responder', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await bus.connect({ agentType: 'rr-worker' });

      bus.reply('rpc:transform', async (payload) => {
        return { transformed: payload.data.toUpperCase() };
      });

      const result = await bus.request('rpc:transform', { data: 'hello' }, { timeout: 2000 });
      expect(result.transformed).toBe('HELLO');
    });

    test('request times out when no responder', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN });
      await bus.connect({ agentType: 'timeout-worker' });

      await expect(
        bus.request('rpc:missing', {}, { timeout: 500 })
      ).rejects.toThrow(/timeout/i);
    });
  });

  describe('Reconnection', () => {
    test('bus reconnects and resubscribes after server drop', async () => {
      bus = new AgentBus({ url: BUS_URL, token: VALID_TOKEN, reconnectInterval: 200 });
      await bus.connect({ agentType: 'reconnect-worker' });

      const reconnectPromise = new Promise(resolve => bus.once('reconnected', resolve));

      bus._simulateDrop();

      await reconnectPromise;
      expect(bus.connected).toBe(true);

      const received = new Promise(resolve => {
        bus.subscribe('channel:post-reconnect', (msg) => resolve(msg));
      });

      await bus.publish('channel:post-reconnect', { alive: true });
      const msg = await received;
      expect(msg.alive).toBe(true);
    });
  });
});