'use strict';

module.exports = CompoundStatement;
/*
 * A simple wrapper around a set of statements
 */
function CompoundStatement(schema) {
  this.statements = [];
  //
  // Execute compound statements as a single batch, ALWAYS. Must ensure
  // determinism for these operations
  //
  this.batch = true;
  this.schema = schema;
  this.table = this.schema.name;
  this.typeOf = this.schema.type;
}

//
// Return the options so we don't unncessarily keep them on the object and
// handle errors
//
CompoundStatement.prototype.init = function (options, entity) {
  return this._init(options || {}, entity) || {};
};

//
// Overrdide this in higher level statements
//
CompoundStatement.prototype._init = function () {
};

CompoundStatement.prototype.add = function (statement) {
  this.statements.push(statement);
};
