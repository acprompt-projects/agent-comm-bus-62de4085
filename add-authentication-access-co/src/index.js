const { AuthMiddleware, ChannelACL } = require('./auth-middleware');

function createAuthBus(bus, options = {}) {
  const acl = options.acl || new ChannelACL();
  const middleware = new AuthMiddleware({ acl, ...options });

  const authBus = {
    acl,
    middleware,

    async connect(token) {
      const result = await middleware.authenticate(token);
      if (!result.authenticated) {
        const err = new Error(`Authentication failed: ${result.error}`);
        err.code = 'AUTH_FAILED';
        throw err;
      }
      return result.agent;
    },

    publish(channel, message, agent) {
      if (!agent || !agent.id) {
        const err = new Error('Valid agent context required');
        err.code = 'AUTH_REQUIRED';
        throw err;
      }
      return bus.publish(channel, message, { agentId: agent.id, token: agent.token });
    },

    subscribe(channel, handler, agent) {
      if (!agent || !agent.id) {
        const err = new Error('Valid agent context required');
        err.code = 'AUTH_REQUIRED';
        throw err;
      }
      try {
        middleware.authorizeSubscribe(agent.id, channel);
      } catch (err) {
        throw err;
      }
      return bus.on(channel, (message) => {
        handler(message);
      });
    },

    revoke(token) {
      middleware.revokeToken(token);
    },

    allow(agentId, channel, actions) {
      acl.allow(agentId, channel, actions);
      return authBus;
    },

    deny(agentId, channel) {
      acl.deny(agentId, channel);
      return authBus;
    }
  };

  return authBus;
}

module.exports = { AuthMiddleware, ChannelACL, createAuthBus };