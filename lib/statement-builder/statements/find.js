const Statement = require('../statement');

class FindStatement extends Statement {
  _init(options, entity) {
    const opts = {};

    //
    // We assess the length of conditions before and after. We ONLY want to end up
    // doing a SELECT * from TABLE if we pass in zero conditions.
    //
    const conditions = options.conditions || entity;
    const condLength = Object.keys(conditions).length;
    //
    // Parse the conditions into the intended data structure we need. See schema
    // code
    //
    opts.conditionals = this.schema.createConditions(conditions);
    //
    // Inspect the query length and see if we intended on passing anything with
    // a zero length, if we did not, we should error.
    //
    if (!Object.keys(opts.conditionals.query).length && condLength) {
      throw new Error(`Insufficient conditions for find, ${JSON.stringify(conditions)}`);
    }
    opts.type = options.type;
    //
    // Transform any fields that were passed in
    //
    opts.fields = this.schema.fixKeys(options.fields || []);

    opts.limit = options.limit;
    opts.allowFiltering = options.allowFiltering;

    return opts;
  }

  build({ conditionals, fields, limit, type, allowFiltering }) {
    //
    // We default to the table set on conditionals if we are doing a find on
    // a lookup table. We establish this when we create conditions so it kind of
    // makes sense.
    //
    const table = conditionals.table || this.table;

    const fieldsCql = (type === 'count')
      ? 'COUNT(*)'
      : this.schema.fieldString(fields);

    this.cql = `SELECT ${fieldsCql} FROM ${table}`;
    this.name += `${fields.sort().join('-') || type}-from-${table}`;

    if (conditionals.query && conditionals.query.length) {
      this.cql += ` WHERE ${conditionals.query.join(' AND ')}`;
      this.name += `-by-${conditionals.fields.sort().join('-')}`;
    }

    //
    // Limit the query
    //
    if (typeof limit === 'number' && limit > 0) {
      this.cql += ` LIMIT ${limit}`;
      this.name += `-limit-${limit}`;
    }

    if (allowFiltering) {
      this.cql += ' ALLOW FILTERING';
      this.name += '-allow-filtering';
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
  }
}

module.exports = FindStatement;
