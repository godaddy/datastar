const Statement = require('../statement');

class CreateStatement extends Statement {
  _init(options, entity) {
    const opts = {};
    const ret = this.schema.validate(this.schema.fixKeys(entity), 'create');
    opts.entity = this.schema.deNull(ret);

    //
    // Allow ttl to be passed into an insert
    //
    if (options.ttl) opts.ttl = options.ttl;

    return opts;
  }

  build(options) {
    const allFields = this.schema.fields();
    const entity = options.entity;
    //
    // Handle lookup table writes.
    //
    const table = options.table || this.table;

    const placeholders = new Array(allFields.length).join(', ?');
    const ttlClause = options.ttl ? ` USING TTL ${options.ttl}` : '';
    this.cql = `INSERT INTO ${table} (${allFields.join(', ')}) VALUES (?${placeholders})${ttlClause};`;

    this.options = { executeAsPrepared: true, queryName: `insert-${this.name}` };
    //
    // Remark: This could be preparsed and put on options so we wouldn't have to know
    // about the schema
    //
    this.params = this.schema.getValues(entity);

    return this;
  }
}

module.exports = CreateStatement;
