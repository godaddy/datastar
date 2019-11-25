const util = require('util');
const Statement = require('../statement');

class RemoveStatement extends Statement {
  _init(options, entity) {
    const opts = {};
  
    const conditions = options.conditions || entity;
  
    opts.conditionals = this.schema.createRemoveConditions(conditions, options.table);
  
    if (this.typeOf(opts.conditionals) === 'error') {
      return opts.conditionals;
    }
  
    if (!Object.keys(opts.conditionals.query).length) {
      return new Error(util.format('Insufficient conditions to remove %j', conditions));
    }
  
    return opts;
  }  

  build(options) {
    const conditionals = options.conditionals;
    //
    // Handle lookup table deletes by being able to pass in the table;
    // Public API is not allowed to do this
    //
    const table = conditionals.table || this.table;
  
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
  }
}

module.exports = RemoveStatement;
