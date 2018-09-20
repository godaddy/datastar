

var async = require('async');

/*
 * Constructor function for the StatementCollection can decide
 * on using a C* batching or non-batching strategy.
 *
 * @param {Driver} Connection which these statements are associated.
 */
var StatementCollection = module.exports = function StatementCollection(connection, strategy) {
  this.statements = [];
  this.connection = connection;
  this.strategy = strategy || 'batch';
  //
  // Default our consistency to local-quorum as it seems reasonable
  //
  this._consistency = 'localQuorum';

  //
  // If strategy is not "batch" then it is the upper bound of
  // concurrency for the number of statements to execute at once.
  //
  if (this.strategy !== 'batch') {
    this.strategy = this.strategy && !isNaN(this.strategy)
      ? this.strategy
      : 5;
  }
};


/**
 * Hava a way to set consistency for a statementCollection that gets submitted
 * @param {Object} consistency - Consistency object
 * @returns {exports} - The statement collection object to be returned
 */
StatementCollection.prototype.consistency = function (consistency) {
  if (!consistency) {
    return this;
  }
  this._consistency = consistency;
  return this;
};

/*
 * Executes the set of statements using the appropriate strategy
 * (batched or non-batched). "Non-batches" means the set of statements
 * is executed in parallel as independent queries.
 */
StatementCollection.prototype.execute = function (callback) {
  if (this.strategy === 'batch') {
    return this.executeBatch(callback);
  }

  var self = this;
  async.eachLimit(
    this.statements,
    this.strategy,
    function (statement, next) {
      var query = statement.batch
        ? self.returnBatch(statement.statements)
        : statement.extendQuery(
          self.connection.beginQuery()
        );

      query.consistency(self._consistency);

      query.execute(next);

    },
    callback
  );
};

/*
 * Executes the set of statements as a Cassandra batch representing
 * a single transation.
 */
StatementCollection.prototype.executeBatch = function (callback) {
  var batch = this.returnBatch(this.statements)
    .consistency(this._consistency);
  //
  // TODO: How do we handle these additional "options":
  // - `.timestamp()`
  // This should be exposed via some option passed to the model in the future
  //

  batch.execute(callback);
};

//
// Recursively build the batch including any nested batch statements that need
// to be built as well. This allows us to have nested batches within an
// execution of just individual statements as well!
//
StatementCollection.prototype.returnBatch = function (statements) {
  var batch = this.connection.beginBatch();
  statements.forEach(function (statement) {
    if (statement.batch) {
      return batch.add(this.returnBatch(statement.statements));
    }
    batch.add(statement.extendQuery(this.connection.beginQuery()));
  }, this);

  return batch;
};

/*
 * Add a statement to the collection associated with this instance
 */
StatementCollection.prototype.add = function (statement) {
  this.statements.push(statement);
};
