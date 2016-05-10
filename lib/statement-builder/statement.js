'use strict';

module.exports = Statement;

/**
 *
 * @param schema {object} - Schema object
 * @constructor Statement
 * @options Object representing what creates a statement
 * @entities (optional) Possible entities to compile into a statement
 * @conditionals (optional) Used to compile into a proper query
 * @fields (optional) Used to compile the beginning of a select to only fetch certain fields
 *
 */

function Statement(schema) {
  //
  // The point here is to internally transform these options into a proper CQL
  // statement with params to be used in priam.
  //

  this.cql = '';
  this.params = [];
  this.options = {};
  this.name = '';
  this.schema = schema;
  this.table = this.schema.name;
  //
  // Proxy typeOf function
  //
  this.typeOf = this.schema.type;
}

//
// Return the options so we don't unncessarily keep them on the object and
// handle errors
//
Statement.prototype.init = function (options, entity) {
  return this._init(options || {}, entity) || {};
};

//
// Overrdide this in higher level statements
//
Statement.prototype._init = function () {
};

/*
 * Adds this Statement instance to a new query against an
 * arbitrary Cassandra connection which could be (or could not be)
 * associated with a StatementCollection, Model, or other utilty.
 */
Statement.prototype.extendQuery = function (query) {
  //
  // Remark: not sure if we can full drop support from reading
  // "cached" queries from disk to support all current scenarios.
  //
  // if (this.filename) {
  //   query = query.namedQuery(this.filename);
  // }

  //
  // Allows for invocation of arbitrary additional `priam`
  // methods as necessary by concrete statements. e.g.
  // `query.first()` or `query.single()` in `FindStatement`
  //
  if (this.mutate) {
    query = this.mutate(query);
  }

  return query
    .query(this.cql)
    .options(this.options)
    .params(this.params);
};
