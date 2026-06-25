const { verifyToken } = require('acprompt-auth-security');

class ChannelACL {
  constructor() {
    this._rules = new Map();
  }

  allow(agentId, channel, actions) {
    if (!this._rules.has(agentId)) this._rules.set(agentId, new Map());
    const agentRules = this._rules.get(agentId);
    agentRules.set(channel, new Set(actions));
    return this;
  }

  deny(agentId, channel) {
    const agentRules = this._rules.get(agentId);
    if (agentRules) agentRules.delete(channel);
    return this;
  }

  isAllowed(agentId, channel, action) {
    const agentRules = this._rules.get(agentId);
    if (!agentRules) return false;
    const actions = agentRules.get(channel);
    if (actions && actions.has(action)) return true;
    const wildcard = agentRules.get('*');
    if (wildcard && wildcard.has(action)) return true;
    return false;
  }

  getPermissions(agentId) {
    const agentRules = this._rules.get(agentId);
    if (!agentRules) return {};
    const result = {};
    for (const [channel, actions] of agentRules) {
      result[channel] = [...actions];
    }
    return result;
  }
}

class AuthMiddleware {
  constructor(options = {}) {
    this.acl = options.acl || new ChannelACL();
    this._revokedTokens = new Set();
    this._tokenCache = new Map();
    this._cacheTtl = options.cacheTtlMs || 60000;
  }

  async authenticate(token) {
    if (this._revokedTokens.has(token)) {
      return { authenticated: false, error: 'Token revoked' };
    }
    const cached = this._tokenCache.get(token);
    if (cached && Date.now() - cached.cachedAt < this._cacheTtl) {
      return { authenticated: true, agent: cached.agent };
    }
    try {
      const decoded = await verifyToken(token);
      const agent = {
        id: decoded.sub || decoded.agentId,
        roles: decoded.roles || [],
        permissions: decoded.permissions || {},
        token
      };
      this._tokenCache.set(token, { agent, cachedAt: Date.now() });
      return { authenticated: true, agent };
    } catch (err) {
      return { authenticated: false, error: err.message || 'Invalid token' };
    }
  }

  revokeToken(token) {
    this._revokedTokens.add(token);
    this._tokenCache.delete(token);
  }

  canPublish(agentId, channel) {
    return this.acl.isAllowed(agentId, channel, 'publish');
  }

  canSubscribe(agentId, channel) {
    return this.acl.isAllowed(agentId, channel, 'subscribe');
  }

  authorizePublish(agentId, channel) {
    if (!this.canPublish(agentId, channel)) {
      const err = new Error(
        `Agent '${agentId}' not authorized to publish on channel '${channel}'`
      );
      err.code = 'UNAUTHORIZED_PUBLISH';
      err.agentId = agentId;
      err.channel = channel;
      throw err;
    }
  }

  authorizeSubscribe(agentId, channel) {
    if (!this.canSubscribe(agentId, channel)) {
      const err = new Error(
        `Agent '${agentId}' not authorized to subscribe on channel '${channel}'`
      );
      err.code = 'UNAUTHORIZED_SUBSCRIBE';
      err.agentId = agentId;
      err.channel = channel;
      throw err;
    }
  }

  wrapBus(bus) {
    const originalOn = bus.on.bind(bus);
    const originalPublish = bus.publish.bind(bus);
    const subscribers = new Map();

    bus.on = (channel, handler, options = {}) => {
      const wrappedHandler = async (message) => {
        const auth = message._auth;
        if (!auth || !auth.agentId) {
          return;
        }
        try {
          this.authorizeSubscribe(auth.agentId, channel);
          handler(message);
        } catch (err) {
          bus.emit('auth:error', {
            code: err.code,
            agentId: err.agentId,
            channel: err.channel,
            message: err.message
          });
        }
      };
      if (!subscribers.has(channel)) subscribers.set(channel, new Map());
      subscribers.get(channel).set(handler, wrappedHandler);
      return originalOn(channel, wrappedHandler);
    };

    bus.publish = (channel, message, authContext) => {
      if (!authContext || !authContext.agentId) {
        const err = new Error('Authentication context required to publish');
        err.code = 'AUTH_REQUIRED';
        throw err;
      }
      this.authorizePublish(authContext.agentId, channel);
      const enriched = { ...message, _auth: authContext };
      return originalPublish(channel, enriched);
    };

    return bus;
  }

  clearCache() {
    this._tokenCache.clear();
  }
}

module.exports = { AuthMiddleware, ChannelACL };