const
  path      = require('path'),
  util      = require('util'),
  Datastar  = require('../../lib'),
  cassandra = require('cassandra-driver');

const model = Datastar.Model;

/*
 * @param {configs} Object
 * Configs which have already been imported.
 */
exports.configs = {};

/*
 * function load (env, callback)
 * Responds with the config from `wsb-dev-test-config`
 */

/* eslint no-process-env: 0*/
exports.load = function (env, callback) {

  if (!callback && typeof env === 'function') {
    callback = env;
    env = process.env.NODE_ENV || 'development';
  }

  env = env === 'dev' ? 'development' : env;
  if (exports.configs[env]) {
    return callback(null, exports.configs[env]);
  }

  function createKeyspace(data) {
    const client = new cassandra.Client({
      contactPoints: data.cassandra.contactPoints,
      localDataCenter: data.cassandra.localDataCenter,
      authProvider: new cassandra.auth.PlainTextAuthProvider(
        data.cassandra.credentials.username,
        data.cassandra.credentials.password
      )
    });

    client.execute('CREATE KEYSPACE IF NOT EXISTS ' + data.cassandra.keyspace + ' WITH replication = {\'class\' : \'SimpleStrategy\', \'replication_factor\' : 1};', function (err) {
      if (err) return callback(err);
      client.shutdown();
      setConfig(data);
    });

  }

  /*
   * function setConfig(data)
   * Sets the config for this env.
   */
  function setConfig(data) {
    exports.configs[env] = data;
    callback(null, exports.configs[env]);
  }

  //
  // If `DATASTAR_CONFIG` is set then load from
  // that file.
  //
  const configFile = process.env.DATASTAR_CONFIG || path.join(__dirname, '..', 'config', 'config.example.json');

  return createKeyspace(require(configFile));
};

/*
 * function debug (obj)
 * Simple debug function when `process.env.DEBUG` is set.
 */
exports.debug = function debug(obj) {
  if (process.env.DEBUG) {
    console.log(util.inspect(obj, { depth: 20, color: true }));
  }
};

/*
 * function createDatastar(opts)
 * Returns a new Datastar instance with the specified opts.
 */
exports.createDatastar = function (opts, Proto) {
  Proto = Proto || Datastar;
  return new Proto(opts);
};

/*
 * function connectDatastar(opts)
 * Returns a new Datastar instance with the specified opts.
 * and then connects
 */
exports.connectDatastar = function (opts, Proto, callback) {
  Proto = Proto || Datastar;
  return new Proto(opts).connect(callback);
};

/*
 * function stubModel()
 * Returns a stubbed Datastar Model.
 */
exports.stubModel = function (sinon) {
  model.before = sinon.stub();
  model.ensureTables = sinon.stub();

  return model;
};
