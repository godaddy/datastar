'use strict';

var util = require('util');
var Statement = require('../statement');

var FindStatement = module.exports = function () {
  Statement.apply(this, arguments);
};

util.inherits(FindStatement, Statement);

FindStatement.prototype._init = function (options, entity) {
  var opts = {};

  //
  // We assess the length of conditions before and after. We ONLY want to end up
  // doing a SELECT * from TABLE if we pass in zero conditions.
  //
  var conditions = options.conditions || entity;
  var condLength = Object.keys(conditions).length;
  //
  // Parse the conditions into the intended data structure we need. See schema
  // code
  //
  opts.conditionals = this.schema.createConditions(conditions);
  if (this.typeOf(opts.conditionals) === 'error') {
    return opts.conditionals;
  }
  //
  // Inspect the query length and see if we intended on passing anything with
  // a zero length, if we did not, we should error.
  //
  if (!Object.keys(opts.conditionals.query).length && condLength) {
    return new Error(util.format('Insufficient conditions for find, %j', conditions));
  }
  opts.type = options.type;
  //
  // Transform any fields that were passed in
  //
  opts.fields = this.schema.fixKeys(options.fields || []);

  opts.limit = options.limit;

  return opts;
};

FindStatement.prototype.build = function (options) {
  var conditionals = options.conditionals;
  var fields = options.fields;
  var limit = options.limit;
  var type = options.type;
  var fieldsCql = '*';
  //
  // We default to the table set on conditionals if we are doing a find on
  // a lookup table. We establish this when we create conditions so it kind of
  // makes sense.
  //
  var table = conditionals.table || this.table;

  if (type === 'count') {
    fieldsCql = 'COUNT(*)';
  } else if (fields && fields.length) {
    fieldsCql = fields.join(', ');
  }

  this.cql = util.format('SELECT %s FROM %s', fieldsCql, table);
  this.name += (fields.sort().join('-') || type) + '-from-' + table;

  if (conditionals.query && conditionals.query.length) {
    this.cql += util.format(' WHERE %s', conditionals.query.join(' AND '));
    this.name += '-by-' + conditionals.fields.sort().join('-');
  }

  //
  // Limit the query
  //
  if (typeof limit === 'number' && limit > 0) {
    this.cql += util.format(' LIMIT %s', limit);
    this.name += util.format('-limit-%s', limit);
  }
  //
  // This should ideally be configurable
  //
  this.options = {
    executeAsPrepared: true,
    queryName: this.name,
    //
    // For streaming and large queries
    //
    autoPage: true
  };
  this.params = conditionals.params;

  if (type === 'first' || type === 'count') {
    this.mutate = function (query) {
      return query.first();
    };
  } else if (type === 'one') {
    this.mutate = function (query) {
      return query.single();
    };
  }

  return this;
};
