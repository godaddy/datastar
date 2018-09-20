

var util = require('util');
var clone = require('clone');
var assign = require('object-assign');
var CompoundStatement = require('../compound-statement');
var CreateStatement = require('./create');
var RemoveStatement = require('./remove');
var Statement = require('../statement');

var mapKeyBadRegex = /--/;
var positiveIntRegex = /^\d+$/;

var UpdateStatement = module.exports = function () {
  CompoundStatement.apply(this, arguments);

  this.buffer = [];
  //
  // Track the index of the statement we need to append to based on the type of
  // operations. Lets map it by type as that should reduce the overall number of
  // statements
  //
  this.index = {};
  this.index.set = 0;
  this.index.list = 0;
  this.index.map = 0;
  this.index.delete = 0;
};

util.inherits(UpdateStatement, CompoundStatement);

//
// For lookup tables this is going to be expensive as we need to do a find
// before this even happens unless we pass in the previous entity
//
UpdateStatement.prototype._init = function (options, entity) {
  var previous,
    opts = {},
    changed,
    prev,
    key;

  entity = this.schema.fixKeys(entity);

  if (options.previous) {
    previous = this.schema.fixKeys(options.previous);
  }

  //
  // Assess whether or not one of our primaryKeys has changed (pertaining to
  // lookup tables). Previous is required to be a fully formed object that
  // contains ALL properties, otherwise we will have a very bad time with this
  // create/delete that needs to happen
  //
  if (this.schema.lookups && options.table) {
    key = this.schema._reverseLookupKeyMap[options.table];
    //
    // If the key exists in the entity that is being updated and it is not
    // equal to that of the previous entity, a primary key has changed
    //
    changed = entity[key] && previous && entity[key] !== previous[key];
  }

  //
  // Pass the changed status to the build function so we know we are going to do
  // a create/delete statement here
  //
  opts.changed = changed;

  //
  // If we have changed, validate that the previous value would be valid for
  // a create operation. This will guarantee that we have a proper previous
  // value passed in and not just a stub which would cause terrible things to
  // happen to the lookup tables as it would be wrong and out of sync
  //
  if (changed) {
    prev = this.schema.validate(this.schema.deNull(previous), 'create');
    if (this.typeOf(prev) === 'error') {
      return prev;
    }
    previous = prev;
  }

  //
  // We need to generate specific conditions
  // Remark: For updating lookup tables, we need to use the previous entity in
  // order to create the conditions so the where clause in finding it is correct
  //
  opts.conditionals = this.schema.createUpdateConditions(previous || entity, options.table);
  //
  // Handle the error case when we do not have the appropriate conditions
  //
  if (this.typeOf(opts.conditionals) === 'error') {
    return opts.conditionals;
  }
  //
  // We need a raw transformed entity
  // TODO: Proper validation on update with the different data structures that
  // can be passed for operations on higher level types, <map> <set> <list>
  // In cases where we have detected a primaryKey change, we create an entity
  // that will be used in a `create` statement instead
  //

  opts.entity = this.schema.deNull(
    changed ? this._entityToReplace(previous, entity) : entity
  );

  if (this.typeOf(opts.entity) === 'error') {
    return opts.entity;
  }

  //
  // Validate and transform the entity
  //
  opts.entity = this.schema.validate(opts.entity, 'update');

  if (this.typeOf(opts.entity) === 'error') {
    return opts.entity;
  }

  //
  // Pass down the table if we are dealing with lookup tables
  //
  opts.table = options.table;

  //
  // Allow ttl to be passed into an update
  //
  if (options.ttl) opts.ttl = options.ttl;

  return opts;
};

//
// Do a merge of the previous and entity while taking into consideration the
// special data-structures we deal with the special commands for sets/lists etc.
//
UpdateStatement.prototype._entityToReplace = function (previous, entity) {
  var self = this;
  //
  // Create a normalized entity using the previous as a reference so that we can
  // just do a proper shallow merge with the previous to get the entity we want
  //
  var normalizedEntity = Object.keys(entity).reduce(function (acc, field) {
    var meta = self.schema.fieldMeta(field);
    var value;

    switch (meta.type) {
      //
      // For a map we need to just merge the values with previous
      //
      case 'map':
        value = assign(previous[field] || {}, entity[field]);
        break;
      //
      // Detect if we have a special version data structure, and take the
      // appropriate actions based on the `add` or `remove`
      //
      case 'set':
        value = self._handleSetOrList(entity[field], value);
        break;
      case 'list':
        value = self._handleList(entity[field], value);
        break;
      default:
        value = entity[field];
        break;
    }
    acc[field] = value;
    return acc;

  }, {});

  //
  // Return the shallow merged version of the entity to put into cassandra
  //
  return assign(previous, normalizedEntity);
};


//
// Remark: We do not do any deletes in update because we are very wary of creating
// tombstones. We use a specific strategy of setting `null-like` characters for
// each type in order to prevent this from happening
//
UpdateStatement.prototype.build = function (options) {
  var conditionals = options.conditionals;
  var entity = options.entity;
  //
  // Remark: If we have a lookup table passed down we use it when considering the
  // filtering
  //
  var isLookup = !!options.table;
  var table = options.table || this.table;

  //
  // This is a special case where the primaryKey has changed which means an
  // update statement will not suffice, we need a delete and a create statement.
  // We ensure that we only do these create and remove statements in the case where
  //
  if (options.changed) {
    return this.replaceLookupRecord(options);
  }

  this.options = { executeAsPrepared: true, queryName: 'update-' + table };

  //
  // Create a criteria object that is used to add the where clause info needed
  // for each statement that gets created
  //
  this.criteria = {};
  this.criteria.cql = conditionals.query.join(' AND ');
  this.criteria.params = conditionals.params;

  Object.keys(entity)
    //
    // Remark: This filtering should be disabled when dealing with updating
    // lookup tables with primary keys.
    //
    // Why does this filter exist? Assumption: We assume that the primary key
    // never changes but this does not hold true when we are dealing with lookup
    // tables in certain cases so we need to handle this here
    //
    .filter(function (field) {
      //
      // Filter out the primary/secondary keys
      // UNLESS we are dealing with lookup tables since we need to update those
      // keys properly
      //
      // When we are a lookup table we only filter out the primary key
      // associated with that table, otherwise cassandra yells at us
      return isLookup
        ? this.schema._reverseLookupKeyMap[table] !== field
        : !this.schema.isKey(field);
    }, this)
    .forEach(function (field) {
      //
      // Grab the value of the update
      //
      var value = entity[field];
      //
      // Get the column metadata for the given field and create the right kind
      // of statement
      //
      var meta = this.schema.fieldMeta(field);

      switch (meta.type) {
        case 'map':
          this.mapUpdate(field, value, meta);
          break;
        case 'set':
          this.setUpdate(field, value, meta);
          break;
        case 'list':
          this.listUpdate(field, value, meta);
          break;
        default:
          this.columnUpdate(field, value, meta);
          break;
      }
    }, this);

  //
  // Iterate through the statements we created, assess them and build the actual
  // statements array if they are valid
  // NOTE: Partial Statements end up being a special kind of statement that has
  // an array as its `cql` property rather than a string to make it more easily
  // extensible. (many many things can be updated in a single statement). The
  // finalizeStatement takes care of normalizing these partials into a proper
  // full statement
  //
  for (var i = 0; i < this.buffer.length; i++) {
    var partialStatement = this.buffer[i];
    if (partialStatement.cql.length) {
      //
      // Build the REAL statement and push it to the array
      //
      this.statements.push(this.finalizeStatement(options, table, partialStatement));
    }
  }
  this.buffer.length = 0;

  return this;
};

//
// This is a special function that creates a create and a remove statement based
// on the options given to be executed in cases where
//
UpdateStatement.prototype.replaceLookupRecord = function (options) {
  this.statements.push(new RemoveStatement(this.schema).build(options));
  this.statements.push(new CreateStatement(this.schema).build(options));
  return this;
};

//
// Handle map updates which receives an object with { key: value }
// as the `value` param
//
UpdateStatement.prototype.mapUpdate = function (field, value, meta) {

  //
  // If the value itself is null, set it to an empty object and set the map
  // equal to that
  //
  if (value === null) {
    value = {};
  }

  if (Array.isArray(value) || !(value && typeof value === 'object')) {
    throw new Error('Tried to insert value "'
      + value + '" into map "'
      + field + '" in table "'
      + this.table + '". Value should be an object.');
  }
  //
  // Remark: In theory this validation should happen earlier and this should simply
  // generate the statement (which is very simple)
  //
  Object.keys(value).forEach(function (mapKey) {
    var mapValue = value[mapKey];
    //
    // TODO: This validation should be handled by joi schema at a higher level
    //
    if (mapKeyBadRegex.test(mapKey)) {
      throw new Error('Tried to insert invalid map key "'
        + mapKey + '" into map "'
        + field + '" in table "'
        + this.table + '".');
    }
    //
    // Strip any undefined values, TODO: This should be done before hand
    //
    if (typeof mapValue === 'undefined') {
      delete value[mapKey];
    }

    //
    // Remark: Since null signifies a delete, we explicitly set the property to null.
    // Since we don't really want to delete (it creates tombstones) we should
    // set this to a value based on the map's valueType before we even get here
    // TODO: See if this can be done in the same statement as the full
    // collection statement. I feel like this value would need to be stripped in
    // any case or would the null be handled correctly if we do it as a standard
    // collection statement?
    // if (mapValue === null) {
    //  statement.cql.push(field + '[\'' + mapKey + '\']' + ' = ?');
    //  statement.params.push(this.schema.valueOf(field, mapValue));
    // }


  }, this);

  this.generateCollectionStatement({
    field: field,
    value: value,
    type: meta.type,
    operator: '+',
    suffix: true
  });

};

//
// Remark: Currently we do not allow objects to be passed in for a set due to schema
// validation, we should consider this for update
//
// value: [] or { add: [], remove: [] }
//
UpdateStatement.prototype.setUpdate = function (field, value, meta) {
  var type = meta.type;

  //
  // Assess the typeof the value that is being passed in for the set-type
  //
  switch (this.typeOf(value)) {
    //
    // Directly set the value when its an array
    //
    case 'array':
      //
      // We just set the array like a regular column
      //
      this.columnUpdate(field, value, meta);
      break;
    case 'object':
      ['add', 'remove'].forEach(function (key) {
        if (!value[key] || !value[key].length) return;
        this.generateCollectionStatement({
          field: field,
          value: value[key],
          type: type,
          operator: key === 'remove' ? '-' : '+',
          suffix: true
        });

      }, this);
      break;
    default:
      //
      // Validation should catch this so this case shouldnt be hit
      //
      throw new Error('Invalid value ' + value + ' for set update on ' + field);
  }
};

//
// List: [] or { prepend: [], append: [], remove: [], index: { idx: value }
//
UpdateStatement.prototype.listUpdate = function (field, value, meta) {
  var type = meta.type;

  switch (this.typeOf(value)) {
    case 'array':
      this.columnUpdate(field, value, meta);
      break;
    case 'object':
      ['prepend', 'append', 'remove']
        .forEach(function (key) {
          //
          // If we don't contain the appropriate keys, do nothing
          //
          if (!value[key] || !value[key].length) return;
          this.generateCollectionStatement({
            field: field,
            value: !Array.isArray(value[key]) ? [value[key]] : value[key],
            type: type,
            operator: key === 'remove' ? '-' : '+',
            suffix: key !== 'prepend'
          });
        }, this);

      //
      // Index operations are a little bit more complex
      //
      if (value.index
        && this.typeOf(value.index) === 'object'
        && Object.keys(value.index).length) {
        this.generateListIndexStatement(field, value.index, meta);
      }
      break;
    default:
      throw new Error('Invalid value ' + value + ' for list update on ' + field);
  }
};

UpdateStatement.prototype.generateListIndexStatement = function (field, map, meta) {
  var statement = this.statement();
  Object.keys(map).forEach(function (idx) {
    var value = map[idx];
    //
    // Don't allow negative indicies
    // Remark/TODO: This could live in actual validation for these more complex types
    //
    if (!positiveIntRegex.test(idx + '')) {
      throw new Error('Tried to insert an invalid index "' + idx + '" into list "' + field + '"');
    }
    statement.cql.push(field + '[' + idx + '] = ?');
    //
    // Remark: Override the hint associated with the `field` because it comes out wrong
    // for this list operation. We pass the listType in this case to override
    // the "hint" because its not an array here
    //
    statement.params.push(this.schema.valueOf(field, value, meta.listType));
  }, this);
};

//
// Update a standard cassandra column
//
UpdateStatement.prototype.columnUpdate = function (field, value) {
  var statement = this.statement();
  statement.cql.push(field + ' = ?');
  //
  // This should be the correct value returned
  //
  statement.params.push(this.schema.valueOf(field, value));
};

//
// Generate the more complicated collection operation statement's used here.
//
UpdateStatement.prototype.generateCollectionStatement = function (opts) {
  var statement = this.statement(this.index[opts.type]);
  //
  // Generate the appropriate statement based on if its a suffix or not for any
  // generic collection
  //
  statement.cql.push(
    opts.field + ' = ' +
    (opts.suffix
      ? (opts.field + ' ' + opts.operator + ' ?')
      : ('? ' + opts.operator + ' ' + opts.field)
    )
  );
  statement.params.push(this.schema.valueOf(opts.field, opts.value));
  //
  // Remark: Only set and list operations need to ensure subsequent commands exist on
  // a new statement
  //
  if (['list', 'set'].indexOf(opts.type) !== -1) this.index[opts.type]++;
};


//
// Return the current statement to modify
// Question: Will index counters of different types end up returning the same statement?
//
UpdateStatement.prototype.statement = function (idx) {
  idx = idx || 0;
  while (idx >= this.buffer.length) {
    this.buffer.push(new PartialStatement());
  }

  return this.buffer[idx];
};

//
// Finalize a statement given a partial statement
//
UpdateStatement.prototype.finalizeStatement = function (options, table, partial) {
  var statement = new Statement(this.schema);
  var ttl = options.ttl ? util.format(' USING TTL %d', options.ttl) : '';
  if (!partial.delete) {
    statement.cql += 'UPDATE ' + table + ttl + ' SET ' +
      partial.cql.join(', ') + ' WHERE ' + this.criteria.cql;
  } else {
    statement.cql += 'DELETE ' + partial.cql.join(', ') + ' FROM ' +
      this.table + ' WHERE ' + this.criteria.cql;
  }
  statement.params = partial.params.concat(clone(this.criteria.params));
  statement.options = this.options;
  return statement;
};

//
// Special statement used for update
//
function PartialStatement() {
  this.cql = [];
  this.params = [];
  this.options = {};
}


//
// Return a proper value when testing for list specific properties that does not
// have shared semantics with set
//
UpdateStatement.prototype._handleList = function handleList(list, prev) {
  var value = prev || [];

  //
  // Run the functions that set and list share first
  //
  value = this._handleSetOrList(list, prev);

  if (list.prepend && Array.isArray(list.prepend)) {
    value.shift.apply(value, list.prepend);
  }

  if (list.index && this.typeOf(list.index) === 'object') {
    Object.keys(list.index).forEach(function (idx) {
      //
      // Don't allow any dangerous operations, and maybe error here
      //
      if (idx >= value.length) return;
      //
      // Set the value to the index value of the array
      //
      value[+idx] = list.index[idx];
    });
  }

  return value;
};
//
// Return the proper value given a set or a list and the previous value
//
UpdateStatement.prototype._handleSetOrList = function handleSetOrList(sol, prev) {
  var value = prev || [];
  var add = sol.add || sol.append;

  if (Array.isArray(sol)) {
    return sol;
  }
  //
  // Handle the add or append case for sets or lists by pushing to the back of
  // the array
  //
  if (add && Array.isArray(add)) {
    value.push.apply(value, add);
  }

  if (sol.remove && Array.isArray(sol.remove)) {
    //
    // Iterate through the remove array and splice out the index if it
    // exists in the previous array. This simulates the Cassandra
    // operation
    //
    sol.remove.forEach(function (key) {
      var idx = value.indexOf(key);
      if (idx === -1) return;
      value.splice(idx, 1);
    });
  }

  return value;
};

