/* eslint no-process-env: 0 */


var util = require('util');
var clone = require('clone');
var Statement = require('../statement');
var With = require('../partial-statements/with');

var TableStatement = module.exports = function () {
  Statement.apply(this, arguments);
};

util.inherits(TableStatement, Statement);

TableStatement.prototype._init = function (options) {
  var alter = options.alter || options.with || {};
  var opts = {};
  var orderBy;
  var w;

  //
  // We want to converge on everything being under alter/with option
  //
  if (options.orderBy || alter.orderBy) {
    //
    // Currently this is expected to be an object with properties
    // { key: 'createdAt', order: 'ascending' }
    //
    orderBy = clone(options.orderBy || alter.orderBy);
    //
    // Map it to the correct string if passed in correctly if exists (its ok for
    // this to be undefined).
    //
    orderBy.order = this.schema.orderMap[orderBy.order && orderBy.order.toLowerCase()];
    //
    // Test if the key exists and returns the transformed key to use if it does,
    // otherwise returns undefined.
    //
    orderBy.key = this.schema.exists(orderBy.key);
    if (!this.schema.exists(orderBy.key)) {
      return new Error(orderBy.key + ' does not exist for the ' + this.name + ' schema');
    }

    alter.orderBy = orderBy;
  }

  if (options.lookupKey) {
    opts.lookupKey = options.lookupKey;
    opts.lookupColumn = this.schema.meta[opts.lookupKey];
    if (['map', 'set'].indexOf(opts.lookupColumn.type) !== -1) {
      return new Error(
        'Creating lookup table with type: '
        + opts.lookupColumn.type);
    }
  }

  opts.useIndex = !!options.useIndex;

  //
  // If we are altering the table with a `with`
  //
  if (alter && Object.keys(alter).length) {
    w = new With(alter);
    if (w.error) return w.error;
    opts.with = w.cql;
  }

  //
  // `ensure` or `drop` currently
  //
  opts.type = options.type;

  var env = process.env.NODE_ENV;
  if (['prod', 'production'].indexOf(env) !== -1
      && opts.type === 'drop'
      && !options.force) {
    return new Error('Please don\'t try and drop your prod tables without being certain');
  }

  return opts;
};

TableStatement.prototype.build = function (options) {
  var schema = options.schema || this.schema;
  var type = options.type;
  var tableName;

  this.options = {
    executeAsPrepared: true,
    queryName: type + '-table-' + schema.name
  };

  tableName = this._computeTable(options);

  this.params = [];

  this.cql = this._compile(options, tableName, options.lookupKey || schema.primaryKeys());

  return this;
};

TableStatement.prototype._computeTable = function (options) {
  var schema = options.schema || this.schema;
  var type = options.type;
  var table;

  this.options.queryName = [type, 'index', schema.name, options.lookupKey].join('-');

  if (!options.lookupKey) return this.table;

  table = this.schema.lookupTables[options.lookupKey];

  if (table) return table;
  //
  // Compute the lookupTable name based on the key and if its used as an index
  // or not
  //
  return options.useIndex
    ? schema.name + '_' + options.lookupKey
    : schema.name + '_by_' + options.lookupKey.replace(/_\w+$/, '');
};

//
// Figure out what statement will be executed
//
TableStatement.prototype._compile = function (options) {
  var fn = this['_' + options.type];
  if (fn) return fn.apply(this, arguments);

  // This shouldn't happen
  throw new Error('Invalid type ' + options.type);
};

/*
 * Drop the table or index
 * @options {Object} options passed in
 * @tableName {String} Name of table or index
 * @returns {String} cql value for statement to be executed
 */
TableStatement.prototype._drop = function (options, tableName) {
  return [
    'DROP',
    (options.useIndex ? 'INDEX' : 'TABLE'),
    tableName
  ].join(' ');
};

TableStatement.prototype._ensure = function (options, tableName, primaryKeys) {
  var schema = options.schema || this.schema;
  var secondaryKeys = schema.secondaryKeys();

  tableName = tableName || this.table;
  primaryKeys = primaryKeys || schema.primaryKeys();

  var cql = '';

  if (options.useIndex) {
    cql += 'CREATE INDEX IF NOT EXISTS ' + tableName
      + ' on ' + schema.name + '(' + primaryKeys + ')';
    return cql;
  }

  cql += 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (\n';

  Object.keys(schema.meta).forEach(function (key) {
    var column = schema.meta[key];
    cql += '  ';
    //
    // Handle all the higher level types
    //
    //
    if (['map', 'set', 'list'].indexOf(column.type) !== -1) {
      cql += key + ' ' + column.type + '<'
        + [].concat(column[column.type + 'Type']).join(',') + '>,\n';
      return;
    }
    cql += key + ' ' + column.type + ',\n';
  }, this);

  //
  // Handle both compoundKeys as well as
  //
  cql += '  PRIMARY KEY (' + (
    schema.compositePrimary
      ? '(' + primaryKeys.join(', ') + ')'
      : primaryKeys
  );

  //
  // Properly support secondary keys / clustering keys
  //
  cql += secondaryKeys && secondaryKeys.length
    ? ', ' + secondaryKeys.join(', ')
    : '';
  //
  // Close keys paren
  //
  cql += ')\n';
  //
  // Close table statement paren
  //
  cql += ')';

  // If we have a with statement to append, lets do that here
  if (options.with) {
    cql += ' ' + options.with;
  }

  cql += ';';
  return cql;
};
