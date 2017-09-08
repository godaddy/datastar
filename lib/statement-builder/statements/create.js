'use strict';

var util = require('util');
var Statement = require('../statement');

var CreateStatement = module.exports = function () {
  Statement.apply(this, arguments);
};

util.inherits(CreateStatement, Statement);

CreateStatement.prototype._init = function (options, entity) {
  var opts = {};
  var ret = this.schema.validate(this.schema.fixKeys(entity), 'create');
  if (this.typeOf(ret) === 'error') {
    return ret;
  }

  opts.entity = this.schema.deNull(ret);

  if (this.typeOf(opts.entity) === 'error') {
    return opts.entity;
  }

  //
  // Allow ttl to be passed into an insert
  //
  if (options.ttl) opts.ttl = options.ttl;

  return opts;
};

CreateStatement.prototype.build = function (options) {
  var allFields = this.schema.fields();
  var entity = options.entity;
  //
  // Handle lookup table writes.
  //
  var table = options.table || this.table;

  this.cql = util.format(
    'INSERT INTO %s (%s) VALUES (?%s)%s;',
    table,
    allFields.join(', '),
    new Array(allFields.length).join(', ?'),
    // conditionally add ttl if it exists
    options.ttl ?  util.format(' USING TTL %d', options.ttl) : ''
  );

  this.options = { executeAsPrepared: true, queryName: 'insert-' + this.name };
  //
  // Remark: This could be preparsed and put on options so we wouldn't have to know
  // about the schema
  //
  this.params = this.schema.getValues(entity);

  return this;
};
