const { PassThrough } = require('stream');

class Priam {

  /*
  * function Priam (opts)
  * Constructor function for the Priam mock responsible for
  * mocking our communication with Cassandra.
  */
  constructor(opts) {
    this.options = opts;
  }

  /*
  * Alias the priam connect function
  */
  connect(keyspace, callback) {
    if (!callback) {
      callback = keyspace;
      keyspace = null;
    }

    setImmediate(callback);
  }

  beginQuery() {
    return new Chainable();
  }
}

/*
 * function beginBatch ()
 * function beginQuery ()
 * Begins a new batch.
 */
Priam.prototype.beginBatch = Priam.prototype.beginQuery;

class Chainable {
  /*
  * function Chainable
  * Constructor function for a mock batch
  * or query.
  */
  constructor() {
    this.statements = [];
  }

  /*
  * function add (statement)
  * Adds the statement to this Chainable instance.
  */
  add(statement) {
    this.statements.push(statement);
    return this;
  };

  stream() {
    const stream = new PassThrough({ objectMode: true });
    stream.end();
    return stream;
  }

  /*
  * function execute (callback)
  * Invokes the callback in the next tick
  */
  execute(callback) {
    setImmediate(callback);
    return this;
  }
}

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

module.exports =  Priam;
