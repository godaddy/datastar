

var util = require('util');
var Statement = require('../statement');

var RemoveStatement = module.exports = function () {
  Statement.apply(this, arguments);
};

util.inherits(RemoveStatement, Statement);

RemoveStatement.prototype._init = function (options, entity) {
  var opts = {};

  var conditions = options.conditions || entity;

  opts.conditionals = this.schema.createRemoveConditions(conditions, options.table);

  if (this.typeOf(opts.conditionals) === 'error') {
    return opts.conditionals;
  }

  if (!Object.keys(opts.conditionals.query).length) {
    return new Error(util.format('Insufficient conditions to remove %j', conditions));
  }

  return opts;
};

RemoveStatement.prototype.build = function (options) {
  var conditionals = options.conditionals;
  //
  // Handle lookup table deletes by being able to pass in the table;
  // Public API is not allowed to do this
  //
  var table = conditionals.table || this.table;

  //
  // THe actual CQL
  //
  this.cql = util.format('DELETE FROM %s', table);
  this.cql += util.format(' WHERE %s', conditionals.query.join(' AND '));

  //
  // Name of the query to pass to priam
  //
  this.name += 'remove-' + table + conditionals.fields.sort().join('-');
  this.options = { executeAsPrepared: true, queryName: this.name };
  this.params = conditionals.params;

  return this;
};
