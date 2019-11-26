const Statement = require('../statement');

class RemoveStatement extends Statement {
  _init(options, entity) {
    const opts = {};

    const conditions = options.conditions || entity;

    opts.conditionals = this.schema.createRemoveConditions(conditions, options.table);

    if (!Object.keys(opts.conditionals.query).length) {
      throw new Error(`Insufficient conditions to remove ${JSON.stringify(conditions)}`);
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
    this.cql = `DELETE FROM ${table}`;
    this.cql += ` WHERE ${conditionals.query.join(' AND ')}`;

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
