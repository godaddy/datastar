const statements = require('./statements');
const CompoundStatement = require('./compound-statement');

/*
 * Constructor function for the StatementBuilder responsible for creating
 * individual statements to be used individually or as part of a
 * StatementCollection.
 *
 * @param {Model}  Model Underlying Model we are building statements for
 * @param {Object} Options for building statements.
 *
 */
const StatementBuilder = module.exports = function StatementBuilder(schema, options) {
  this.schema = schema;
  this.options = options || {};
  this.typeOf = this.schema.type;

  //
  // Methods that require us to build a specific compound statement to execute
  // multiple queries on multiple tables
  //
  this.multiMethods = ['create', 'update', 'remove'].reduce(function (acc, key) {
    acc[key] = true;
    return acc;
  }, {});
};

//
// Take the conditions and fields and build CQL from it
//
['create', 'update', 'remove', 'find', 'table', 'alter'].forEach(function (action) {
  StatementBuilder.prototype[action] = function (options, entity) {
    options = options || {};
    options.action = action;
    //
    // Construct and build the statement
    //
    const statement = new statements[action](this.schema);
    const opts = statement.init(options, entity);

    //
    // If we don't have lookup tables, return the standard built statement,
    // otherwise build a compound statement directly here.
    //
    if (!this.schema.lookups || !this.multiMethods[action]) {
      return statement.build(opts);
    }

    //
    // Build a compound statement and add the initial statement to it
    //
    const compound = new CompoundStatement(this.schema);
    compound.add(statement.build(opts));
    //
    // Use the lookup tables map and build the statements.
    // Since we already validated the first statement here, its impossible to
    // error and we just use the same
    //
    const lookupMap = this.schema.lookupTables;
    const keys = Object.keys(lookupMap);

    for (let i = 0; i < keys.length; i++) {
      const table = lookupMap[keys[i]];
      //
      // Clone the initialized options and add the new table to them
      //
      const stmnt = new statements[action](this.schema);
      //
      // Set table for both steps for the various statements. We does this
      // weirdly because we dont want a statement to accept a table from
      // a user's input. This would cause unintended behavior
      //
      options.table = table;
      //
      // This can never error because we ran `ini` on these SAME set of options
      // previously and only added a table property
      //
      const op = stmnt.init(options, entity);
      op.table = table;
      compound.add(stmnt.build(op));
    }

    //
    // Return an error if we error a compound statement
    //
    return compound;
  };
});



