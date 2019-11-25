/* eslint no-process-env: 0 */

const clone = require('clone');
const Statement = require('../statement');
const With = require('../partial-statements/with');

class TableStatement extends Statement {
  _init(options) {
    const alter = options.alter || options.with || {};
    const opts = {};
  
    //
    // We want to converge on everything being under alter/with option
    //
    if (options.orderBy || alter.orderBy) {
      //
      // Currently this is expected to be an object with properties
      // { key: 'createdAt', order: 'ascending' }
      //
      const orderBy = clone(options.orderBy || alter.orderBy);
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
      const w = new With(alter);
      if (w.error) return w.error;
      opts.with = w.cql;
    }
  
    //
    // `ensure` or `drop` currently
    //
    opts.type = options.type;
  
    const env = process.env.NODE_ENV;
    if (['prod', 'production'].indexOf(env) !== -1
        && opts.type === 'drop'
        && !options.force) {
      return new Error('Please don\'t try and drop your prod tables without being certain');
    }
  
    return opts;
  }  

  build(options) {
    const schema = options.schema || this.schema;
    const type = options.type;
  
    this.options = {
      executeAsPrepared: true,
      queryName: type + '-table-' + schema.name
    };
  
    const tableName = this._computeTable(options);
  
    this.params = [];
  
    this.cql = this._compile(options, tableName, options.lookupKey || schema.primaryKeys());
  
    return this;
  }

  _computeTable(options) {
    const schema = options.schema || this.schema;
    const type = options.type;
  
    this.options.queryName = [type, 'index', schema.name, options.lookupKey].join('-');
  
    if (!options.lookupKey) return this.table;
  
    const table = this.schema.lookupTables[options.lookupKey];
  
    if (table) return table;
    //
    // Compute the lookupTable name based on the key and if its used as an index
    // or not
    //
    return options.useIndex
      ? schema.name + '_' + options.lookupKey
      : schema.name + '_by_' + options.lookupKey.replace(/_\w+$/, '');
  }

  //
  // Figure out what statement will be executed
  //
  _compile(options) {
    const fn = this['_' + options.type];
    if (fn) return fn.apply(this, arguments);

    // This shouldn't happen
    throw new Error('Invalid type ' + options.type);
  }

  /*
  * Drop the table or index
  * @options {Object} options passed in
  * @tableName {String} Name of table or index
  * @returns {String} cql value for statement to be executed
  */
  _drop(options, tableName) {
    return [
      'DROP',
      (options.useIndex ? 'INDEX' : 'TABLE'),
      tableName
    ].join(' ');
  }

  _ensure(options, tableName, primaryKeys) {
    const schema = options.schema || this.schema;
    const secondaryKeys = schema.secondaryKeys();
  
    tableName = tableName || this.table;
    primaryKeys = primaryKeys || schema.primaryKeys();
  
    let cql = '';
  
    if (options.useIndex) {
      cql += 'CREATE INDEX IF NOT EXISTS ' + tableName
        + ' on ' + schema.name + '(' + primaryKeys + ')';
      return cql;
    }
  
    cql += 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (\n';
  
    Object.keys(schema.meta).forEach(function (key) {
      const column = schema.meta[key];
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
  }
}

module.exports = TableStatement;
