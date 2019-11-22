var through = require('through2');
/*
 * function Priam (opts)
 * Constructor function for the Priam mock responsible for
 * mocking our communication with Cassandra.
 */
var Priam = module.exports = function Priam(opts) {
  this.options = opts;
};

/*
 * Alias the priam connect function
 */
Priam.prototype.connect = function (keyspace, callback) {
  if (!callback) {
    callback = keyspace;
    keyspace = null;
  }

  setImmediate(callback);
};

/*
 * function beginBatch ()
 * function beginQuery ()
 * Begins a new batch.
 */
Priam.prototype.beginBatch =
  Priam.prototype.beginQuery = function () {
    return new Chainable();
  };

/*
 * function Chainable
 * Constructor function for a mock batch
 * or query.
 */
function Chainable() {
  this.statements = [];
}

/*
 * function add (statement)
 * Adds the statement to this Chainable instance.
 */
Chainable.prototype.add = function (statement) {
  this.statements.push(statement);
  return this;
};
Chainable.prototype.stream = function () {
  var stream = through.obj();
  stream.end();
  return stream;
};

/*
 * function execute (callback)
 * Invokes the callback in the next tick
 */
Chainable.prototype.execute = function (callback) {
  setImmediate(callback);
  return this;
};

/*
 * function query (cql)
 * function options (obj)
 * function params (obj)
 * function single() -> changes how results are displayed from query
 * function first() -> changes how results are displayed from query
 * Invokes the callback in the next tick
 */
Chainable.prototype.single =
  Chainable.prototype.consistency =
    Chainable.prototype.first =
      Chainable.prototype.query =
        Chainable.prototype.options =
          Chainable.prototype.params = function () {
            return this;
          };

