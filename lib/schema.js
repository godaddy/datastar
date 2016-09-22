'use strict';

var util      = require('util'),
    uuid      = require('uuid'),
    clone     = require('clone'),
    snakeCase = require('to-snake-case'),
    camelCase = require('to-camel-case'),
    assign    = require('object-assign'),
    priam     = require('priam'),
    joi       = require('joi-of-cql');

var dataTypes = priam.dataTypes,
    TimeUuid  = priam.valueTypes.TimeUuid;

var invalidChar = /\W/;

module.exports = Schema;

/**
 * In this method we are going to create a denormalized structure from the
 * schema representation that we type out as JSON. This structure should be easy
 * to ask questions to and lookup various properties, etc.
 *
 * @param {string} name- Schema name
 * @param {Object} schema - Schema object
 * @param {String} schema.name - name of the table
 * @param {Array} schema.keys - Primary and secondary keys of a schema
 * @param {Object} schema.columns - An object of column names to their type
 * @param {Object} schema.maps - An object of the column names that are a special map type with their type as value
 * @param {Object} schema.sets - An object of the column names that are a special set type with their type as value
 * @constructor
 */

function Schema(name, schema) {
  if (!this) return new Schema(name, schema);

  if (!name || invalidChar.test(name))
    throw new Error('Invalid character in schema name ' + name + ', use snake_case');

  //
  // Remark: We lowercase this by default for easier consistency
  //
  this.name = name.toLowerCase();
  //
  // Special operators used for timuuids along with associated functions
  // when building queries
  //
  this.operators = {
    gt: '>',
    gte: '>=',
    lt: '<',
    lte: '<='
  };

  //
  // A mapping for possible keys that can be passed in for defining order
  //
  this.orderMap = {
    ascending: 'ASC',
    asc: 'ASC',
    desc: 'DESC',
    descending: 'DESC'
  };

  this.cqlFunctions = {
    timeuuid: {
      gt: time('min'),
      gte: time('min'),
      lt: time('max'),
      lte: time('max')
    }
  };

  //
  // Keys used when defaulting values to generate a number
  //
  this.generateKeysLookup = ['uuid_v4', 'uuid_empty', 'date_now']
    .reduce(function (acc, type) {
        acc[type] = defaultValue(type);
        return acc;
      }, {}
    );

  //
  // Store a reference to the original joi schema thats passed in
  //
  this.joi = schema;
  //
  // We default to having different validators based on the `type`
  //
  this.validator = {
    create: schema,
    update: schema
  };

  this._columns = this.meta = schema.toCql();
  this._aliases = schema.aliases() || {};
  var keys = schema.clusteringKey();
  if (!Array.isArray(keys)) {
    keys = [keys];
  }

  var pKey = schema.partitionKey();
  //
  // If there is no partitionKey, throw an error because the schema is not valid
  //
  if (!pKey || !pKey.length) throw new Error('You must define a partitionKey on your schema');

  keys.unshift(pKey);
  this._originalKeys = keys;

  //
  // Set the primary and secondary keys
  //
  this._primaryKeys = this._originalKeys[0];
  this._secondaryKeys = this._originalKeys.slice(1);

  //
  // The flattened array of all the necessary keys that are required
  //
  this._keys = this.primaryKeys().concat(this._secondaryKeys);
  //
  // Indication that we have a compound primary/partition key
  //
  this.compositePrimary = this.primaryKeys().length >= 2;

  //
  // Primary or secondary key lookup table
  //
  this._keysLookup = createLookup(this._keys);

  //
  // Lookup for primaryKeys
  //
  this._primaryKeysLookup = createLookup(this.primaryKeys());

  //
  // Secondary Keys lookup.
  // Remark: Not sure if there can be multiple of these but seems possible?
  //
  this._secondaryKeysLookup = createLookup(this._secondaryKeys);

  // Set our list of keys as "columnKeys"
  //
  this._columnKeys = Object.keys(this.meta);

  //
  // We realize that we store aliases in a way that is backwards when
  // considering it as a lookup table to the type of key actually listed in the
  // schema, so lets reverse it to be a proper lookup table for the case that we
  // use. Keep around the original as well.
  //
  // Example: { id: artist_id } is the standard schema.aliases
  // We use it for proper lookups as { artist_id: id } in mappedFields
  //
  // This means that any object passed in with a key `id` will be converted to
  // the real value `artist_id` when being inserted. This CAN also be
  // considered when retransforming on the way out.
  //
  //
  this._aliasesReverse = Object.keys(this._aliases)
    .reduce(function (acc, key) {
      var newKey = this._aliases[key];
      acc[newKey] = key;
      return acc;
    }.bind(this), {});

  //
  // If lookupKeys exist in the schema, setup the proper properties to handle
  // those cases
  //
  var badLookups = this.setLookupKeys(schema.lookupKeys());

  if (badLookups) {
    throw badLookups;
  }
}

//
// Require some set of keys to generate another joi schema
//
Schema.prototype.requireKeys = function (keys) {
  var self = this;
  return keys.reduce(function (memo, key) {
    if (!self.meta[key] || !self.meta[key].default) {
      memo[key] = joi.any().required();
    }
    return memo;
  }, {});
};


//
// Create a separate Lookup table JUST for Lookup tables. Yes confusing I know,
// Object lookup for actual cassandra lookup tables. This should default to
// lookupKeys/lookupTables if it is an object
//
Schema.prototype.setLookupKeys = function (lookupKeys) {
  var self = this;
  //
  // Return an error to be thrown if we are a compositePrimary key and we are
  // given lookupKeys as that is something we do not support
  //
  if (this.compositePrimary && lookupKeys &&
    (this.type(lookupKeys) === 'array' && lookupKeys.length !== 0)
    || (this.type(lookupKeys) === 'object' && Object.keys(lookupKeys).length !== 0))
    return new Error('You cannot create a lookup table with a compound key');

  lookupKeys = this.fixKeys(lookupKeys) || {};
  this.lookupTables = this.type(lookupKeys) === 'object'
    ? lookupKeys
    : lookupKeys.reduce(function (acc, key) {
    acc[key] = self.name + '_by_' + key;
    return acc;
  }, {});

  lookupKeys = Object.keys(this.lookupTables);

  //
  // If there are any lookup keys that do not exist on this
  // Schema then return an error accordingly
  //
  var missingLookupKeys = lookupKeys.filter(function (key) {
    return !this.exists(key);
  }, this);

  if (missingLookupKeys.length) {
    return new Error('Invalid lookup keys: ' + missingLookupKeys.join(', '));
  }

  //
  // Reverse lookup of key -> tableName to tableName -> key. e.g.
  //
  //  {
  //    "model_by_prop1": "prop1",
  //    "model_by_prop2": "prop2"
  //  }
  //
  this._reverseLookupKeyMap = lookupKeys
    .reduce(function (acc, key) {
      var table = self.lookupTables[key];
      acc[table] = key;
      return acc;
    }, {});

  //
  // Set a property on the schema that tells us if we have lookup tables we need
  // to write to.
  //
  this.lookups = !!lookupKeys.length;

  //
  // Setup the requiredKeys lookup. When we are dealing with lookup tables we
  // need to require all the primarykeys associated
  //
  this._requiredKeysLookup = createLookup(lookupKeys.concat(this.keys()));
  this._requiredKeys = Object.keys(this._requiredKeysLookup);
  //
  // Attach any extra restrictions for the create schema
  //
  if (this._requiredKeys.length) {
    this.validator.create = this.validator.create.concat(
      joi.object(
        this.requireKeys(this._requiredKeys)
      )
    );
  }
};

//
// Validate and default things
//
Schema.prototype.validate = function (entity, type) {
  type = type || 'update';
  var result = joi.validate(entity, this.validator[type], { context: { operation: type } });
  return result.error || result.value;
};

//
// Test if the key exists and returns the transformed key to use if it does,
// otherwise returns undefined. This requires us
// to transform the key to snake_case as well as remap any aliases so we can
// specify the key as a standard camelCase key when passing in any options.
//
Schema.prototype.exists = function (key) {
  var transformed = this.fixKeys(key);

  return !this.meta[transformed] ? null : transformed;
};

//
// Transform an entity, an object of conditions or an array of fields to have the proper
// keys that cassandra expects (snake_case, unalias)
//
Schema.prototype.fixKeys = function (entity) {
  entity = entity || {};

  if (entity.isDatastar) {
    entity = entity.attributes.data;
  }

  if (this.type(entity) === 'object') {
    return Object.keys(entity).reduce(function (acc, key) {
      //
      // If we have an alias, check it and convert it to what we expect in C*
      //
      var mappedKey = snakeCase(key);
      var alias = this._aliases[mappedKey];
      if (alias) {
        acc[alias] = entity[key];
      } else {
        acc[mappedKey] = entity[key];
      }

      return acc;
    }.bind(this), {});
  }

  //
  // If we have an array, this is an array of fields for doing "selects"
  //
  if (Array.isArray(entity)) {
    return entity.map(this.fixKeys, this);
  }

  //
  // IDK why this would happen but this is an easy case
  //
  if (this.type(entity) === 'string') {
    var mapped = snakeCase(entity);
    return this._aliases[mapped]
      ? this._aliases[mapped]
      : mapped || entity;
  }

  //
  // If we meet 0 conditions we just return what we got, this maybe should be an
  // error? Idk, this is just a weird thing in general
  //
  return entity;

};

//
// Transform in the opposite direction of transform by remapping snakeCase back
// to camelCase
//
Schema.prototype.toCamelCase = function (entity) {
  entity = entity || {};

  if (this.type(entity) === 'object') {
    return Object.keys(entity).reduce(function (acc, key) {
      //
      // If we have an alias, check it and convert it to what we
      var mappedKey = camelCase(this._aliasesReverse[key] || key);

      acc[mappedKey] = entity[key];

      return acc;
    }.bind(this), {});
  }

  //
  // If we have an array, this is an array of fields for doing "selects"
  //
  if (Array.isArray(entity)) {
    return entity.map(function (field) {
      return camelCase(this._aliasesReverse[field] || field);
    }, this);
  }

  //
  // IDK why this would happen but this is an easy case
  //
  if (this.type(entity) === 'string') {
    return camelCase(this._aliasesReverse[entity] || entity);
  }

  //
  // If we meet 0 conditions we just return what we got, this maybe should be an
  // error? Idk, this is just a weird thing in general
  //
  return entity;
};

//
// Generate a conditions object given a value assumed to be the primary key
//
Schema.prototype.generateConditions = function (value) {
  var self = this;
  var primaries = this.primaryKeys();

  if (primaries.length > 1) {
    return new Error(util.format('More conditions required %s', primaries.join(', ')));
  }

  //
  // Return an object with the single primaryKey with the correct case assigned
  // to the value passed in. Allows us to support passing a string for findOne
  //
  return primaries.reduce(function (acc, key) {
    acc[self.toCamelCase(key)] = value;
    return acc;
  }, {});
};

//
// Return both primary and secondary keys
//
Schema.prototype.keys = function () {
  return this._keys;
};

//
// Returns whether or not it is a primary or secondary key
//
Schema.prototype.isKey = function (key) {
  return this._keysLookup[key];
};

//
// Return the column type for the given
//
Schema.prototype.fieldMeta = function (field) {
  return this.meta[field];
};

Schema.prototype.prepareForUse = function (data) {
  return this.convert(this.fixKeys(data), 'deserialize');
};

// unknown use case
Schema.prototype.prepareForSerialization = function (data) {
  return this.convert(this.fixKeys(data), 'serialize');
};

Schema.prototype.convert = function convert(data, converter) {
  var meta = this.meta;
  Object.keys(meta).forEach(function (key) {
    if (meta[key][converter]) {
      try {
        data[key] = meta[key][converter](data[key]);
      } catch (e) {
        // ignored on purpose
        // we should log this invalid data
      }
    }
  });
  return data;
};

//
// Return the primaryKey based on what type it is which is probably an array.
// Handle the other case as well
//
Schema.prototype.primaryKeys = function () {
  return Array.isArray(this._primaryKeys) && this._primaryKeys.length
    ? this._primaryKeys
    : [this._primaryKeys];
};

Schema.prototype.secondaryKeys = function () {
  return this._secondaryKeys;
};

Schema.prototype.fields = function () {
  return this._columnKeys;
};

Schema.prototype.fieldString = function (fieldList) {
  if (!Array.isArray(fieldList) || !fieldList.length) {
    fieldList = this.fields();
  }
  return fieldList
    .map(function (fieldName) {
      return fieldName && ('"' + fieldName + '"');
    })
    .join(', ');
}

//
// Return all fields, we are going to default to dealing with this as camelCase
//
Schema.prototype.mappedFields = function () {
  if (!this._mappedFields) {
    this._mappedFields = this._columnKeys.map(function (key) {
      //
      // CamelCase and replace alias with actual key name if it exists
      //
      return camelCase(this._aliasesReverse[key] || key);
    }, this);
  }

  return this._mappedFields;
};

//
// Appropriate typeof checking
//
Schema.prototype.type = function type(of) {
  return Object.prototype.toString.call(of).slice(8, -1).toLowerCase();
};

//
// Remark: Create conditions that are meant to be directed at the primary table if there
// is a lookup table situtation. We filter based on the remove logic and do not
// parse into conditionals as this gets passed directly to find
//
Schema.prototype.filterPrimaryConditions = function (conditions) {
  return this.toCamelCase(this.filterRemoveConditions(this.fixKeys(conditions)));
};

//
// Evaluate if we have sufficient conditions for the remove we are executing and
// return them
//
Schema.prototype.createRemoveConditions = function (conditions, table) {
  var transformed = this.fixKeys(conditions);
  //
  // If we are a lookup table and insufficient conditions are passed to execute
  // the queries to ALL the lookup tables, just error for simplicity now.
  //
  if (!this.sufficientRemoveConditions(transformed)) {
    return new Error('Must pass in all primary keys when using lookup tables');
  }

  conditions = this.filterRemoveConditions(transformed, table);

  var conditionals = this.parseConditions(conditions);
  conditionals.table = table;

  return conditionals;
};

//
// Evaluate if we have sufficient conditions for the remove we are executing and
// return them
//
Schema.prototype.createUpdateConditions = function (conditions, table) {
  var transformed = this.fixKeys(conditions);
  //
  // If we are a lookup table and insufficient conditions are passed to execute
  // the queries to ALL the lookup tables, just error for simplicity now. Also
  // handle the case where we do not have sufficient keys for a query, (need all
  // primary keys or both secondary and primary)
  //
  if (!this.sufficientUpdateConditions(transformed)) {
    return new Error(util.format('All necessary primary keys must be passed in, given: %j', conditions));
  }

  conditions = this.filterRemoveConditions(transformed, table);

  var conditionals = this.parseConditions(conditions);
  conditionals.table = table;

  return conditionals;
};

//
// Ensure we have sufficient keys to do an update operation
//
Schema.prototype.sufficientUpdateConditions = function (conditions) {
  var keys = this.lookups ? this.keys().concat(Object.keys(this.lookupTables)) : this.keys();
  return keys.every(function (key) {
    return !!conditions[key];
  });
};

//
// DE-Null the entity, meaning translate known types into our defined null
// equivalents. We expect to receive a fully transformed object with snake case
// keys here. We use a for loop since we do too many iterations over the object
// in this process
//
Schema.prototype.deNull = function (entity) {
  var keys = Object.keys(entity);
  var error;

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    var value = entity[key];
    var meta = this.fieldMeta(key);
    if (!meta) {
      error = new Error(util.format('%s is not found in the schema', key));
      break;
    }

    entity[key] = this.nullToValue(meta, value);
  }

  return error || entity;
};

Schema.prototype.hasAllRequiredKeys = function (entity, previous) {
  return entity && this.type(this.validate(assign(clone(entity), previous || {}), 'update')) !== 'error';
};

//
// detect both empty string and null as a bad uuid value since cassandra will
// give us weird errors if we try and insert an empty string
//
function isBadUuid(value) {
  return value === null || (typeof value === 'string' && value.length === 0);
}

//
// Adjust detected values that are `null` and map them to a `null-like` value.
// TODO: Should we iterate through maps and sets and adjust accordingly as well?
//
Schema.prototype.nullToValue = function (meta, value) {
  var type = meta.type,
      self = this;

  if ((type === 'text' || type === 'ascii') && value === null) {
    // null text values will create tombstones in Cassandra
    // We will write a null string instead.
    return '\x00';
  }
  if ((type === 'uuid' || type === 'timeuuid') && isBadUuid(value)) {
    // null uuid values will create tombstones in Cassandra
    // We will write a zeroed uuid instead.
    return this.generateKeysLookup.uuid_empty();
  }
  if (type === 'timestamp' && value === null) {
    // null timestamp values will create tombstones in Cassandra
    // We will write a zero time instead.
    return new Date(0);
  }
  if (type === 'map') {
    return Object.keys(value).reduce(function reduce(memo, key) {
      memo[key] = self.nullToValue({ type: meta.mapType[1] }, value[key]);
      return memo;
    }, {});
  }
  if (type === 'set' || type === 'list') {
    // Sets are an odd edge case here, it can be an array or an object who's
    // values are sit in an add and/or remove property. This means we need to
    // a bit more work updating this data structure.
    if (this.type(value) === 'object') {
      ['add', 'remove'].forEach(function each(method) {
        if (method in value) value[method] = value[method].map(function map(value) {
          return self.nullToValue({ type: meta.setType }, value);
        });
      });

      return value;
    } else {
      return value.map(function map(value) {
        return self.nullToValue({ type: meta.setType }, value);
      });
    }
  }

  return value;

};

//
// RE-Null the entity. This translates the defined null equivalents
// into an actual null value for the consumer to use.
//
Schema.prototype.reNull = function (entity) {
  var keys = Object.keys(entity);

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    entity[key] = this.valueToNull(entity[key]);
  }

  return entity;
};

//
// Detect our `null-like` values and return null if applicable
//
Schema.prototype.valueToNull = function (value) {
  if (value === '\x00') {
    return null;
  }
  if (value === '00000000-0000-0000-0000-000000000000') {
    return null;
  }

  var type = this.type(value);

  if (type === 'date' && value.getTime() === 0) {
    return null;
  }
  if (type === 'array') {
    for (var i = 0, l = value.length; i < l; i++) {
      value[i] = this.valueToNull(value[i]);
    }

    return value;
  }
  if (type === 'object') {
    var keys = Object.keys(value);

    for (var i = 0, l = keys.length; i < l; i++) {
      value[keys[i]] = this.valueToNull(value[keys[i]]);
    }

    return value;
  }

  return value;
};


//
// Assess if we have sufficient conditions during our pre-remove check from
// a table with a lookup table. This will let our user know if they are trying
// to do something they can't do based on how they defined lookup tables
//
Schema.prototype.sufficientRemoveConditions = function (conditions) {
  var keys = this.lookups
    ? Object.keys(this.lookupTables).concat(this.primaryKeys())
    : this.primaryKeys();

  return keys.every(function (key) {
    return !!conditions[key];
  });
};

//
//
// These are conditions specific for the remove functionality in the case where
// we are removing from a bunch of lookup tables. Also handles the generic case
//
Schema.prototype.filterRemoveConditions = function (conditions, table) {
  //
  // Filter the conditions and pluck the appropriate primary key and secondary
  // keys based on the table
  //
  return Object.keys(conditions)
    .filter(function (key) {
      //
      // Only allow secondary keys or the appropriate primary key. If a table is
      // passed, we check the lookup table keys as well
      //
      return (table
          ? this._reverseLookupKeyMap[table] === key
          : this._primaryKeysLookup[key])
        || this._secondaryKeysLookup[key];
    }, this)
    .reduce(function (acc, key) {
      acc[key] = conditions[key];
      return acc;
    }, {});
};

//
// Remark: Transform the keys and then filter out any keys that are not the
// primary/secondary keys that are used as conditions to query on (creating
// the where clause)
//
Schema.prototype.filterConditions = function (conditions) {
  var table;
  var primaries = [];

  var filtered = Object.keys(conditions)
    .filter(function (key) {
      //
      // If it exists as a primary or secondary key, we keep it and dont filter
      //
      var exists = !!this._keysLookup[key];
      if (this._primaryKeysLookup[key]) primaries.push(key);
      //
      // Check if its part of a lookup table
      //
      table = this.lookupTables[key];
      if (table) primaries.push(key);

      return exists || !!table;
    }, this)
    .reduce(function (acc, key) {
      acc[key] = conditions[key];
      return acc;
    }, {});

  //
  // Return an error if there are more than one primary key being used,
  // meaning we have conflicting lookup tables. Technically we dont need to
  // error, we just filter out or delete one of the keys from the filtered
  // object
  //
  if (primaries.length > 1 && !this.compositePrimary) {
    return new Error('There can only be 1 primary key in a query, found ' + primaries.length + ' ' + primaries);
  }

  return { table: table, conditions: filtered };

};
//
// Create conditions based on an entity or conditions. Optional type paremeter
// can be passed as there is one case we don't want lookup table primary keys to be
// considered valid conditions (remove);
//
Schema.prototype.createConditions = function (conditions) {
  var opts = this.filterConditions(this.fixKeys(conditions));
  //
  // We can error if we try and specify 2 keys which are for conflicting lookup
  // tables. We can only query one
  //
  if (this.type(opts) === 'error') {
    return opts;
  }

  var conditionals = this.parseConditions(opts.conditions);
  //
  // Pass back the table so we can override the standard table after we have
  // parsed the conditions
  //
  conditionals.table = opts.table;
  return conditionals;
};

//
// Parse the conditions into array objects to be used later on
//
Schema.prototype.parseConditions = function (conditions) {
  //
  // Create a data structure
  //
  var conditionals = {
    //
    // The keys that get mapped into the where clause
    //
    query: [],
    //
    // Hints based on parameters
    //
    hints: [],
    //
    // Actual parameter values associated with the query
    //
    params: [],
    //
    // Special routing indexes for parameters that are primaryKeys
    //
    routingIndexes: [],
    //
    // A simple array of field names (i.e. key names) for
    // bookkeeping / logging purposes.
    //
    fields: []
  };

  //
  // Create an array of `where` objects which have a `query` and `param`
  // property as well as the original `field` and `value` i guess?
  //
  Object.keys(conditions).forEach(function (field) {
    var value = conditions[field];
    conditionals.fields.push(field);
    conditionals.query.push(this._getQuery(field, value));

    //
    // Do valueOf on the params to get the value expected by priam.
    // Whats returned by this._getParams is actually the proper value for the
    // query
    //
    conditionals.params.push(this.valueOf(field, this._getParams(field, value)));

  }, this);

  return conditionals;
};

//
// Return the params based on the given entity
//
Schema.prototype.getValues = function (entity, fields) {
  fields = fields || this.fields();

  //
  // Populate all fields (i.e. columns) with
  // any values from the entity. If a value for
  // a particular column is not present we set
  // it EXPLICITLY to `null`.
  //
  return fields.map(function (field) {
    var value = null;
    if (entity.hasOwnProperty(field)) {
      value = entity[field];
    }

    return this.valueOf(field, value);
  }, this);
};

//
// Bit of a hack that returns the data structure expected by priam
//
Schema.prototype.valueOf = function (field, value, type) {
  return {
    value: value,
    hint: this._mapFieldHint(
      type ? type : this._getFieldHint(field)
    ),
    isRoutingKey: this.primaryKeys().indexOf(field) !== -1
  };
};

//
// Add the column names and aliases from the schema definition as
// property getters/setters for the data being modeled by this object
//
Schema.prototype.buildProperties = function () {
  var columns = Object.keys(this.meta);
  var aliasesOf = this._aliasesReverse;

  var definitions = columns.reduce(function (memo, name) {
    name = camelCase(aliasesOf[name] || name);
    memo[name] = {
      get: function () {
        return this.attributes.get(name);
      },
      set: function (value) {
        return this.attributes.set(name, value);
      },
      enumerable: true,
      configurable: true
    };
    return memo;
  }, {});

  return definitions;
};

//
//
Schema.prototype._getQuery = function (field, values) {
  var value;
  if (Array.isArray(values)) {
    if (values.length > 1) {
      return util.format('%s IN (%s)', field, '?' + new Array(values.length).join(', ?'));
    }
    value = values[0];
  } else if (this.type(values) === 'object') {
    value = Object.keys(values)
      .map(function (name) {
        var op = this.operators[name];

        return op
          ? util.format('%s %s ?', field, op)
          : null;
      }, this)
      .filter(Boolean)
      .join(' AND ');

    return value || null;
  } else {
    value = values;
  }

  return this.type(value) === 'string' || this.type(value) === 'number'
    ? util.format('%s = ?', field)
    : null;
};

//
// Transform parameters based on the field passed in and the value associated
// with the field
//
Schema.prototype._getParams = function (field, values) {
  var value;

  if (Array.isArray(values)) {
    values = values.slice(0);
    if (values.length > 1) {
      return values;
    }
    value = values[0];
  } else if (this.type(values) === 'object') {
    value = Object.keys(values)
      .map(function (name) {
        var op   = this.operators[name],
            type = this.meta[field].type;

        if (!op) {
          return null;
        }

        return convertRangeType(this.cqlFunctions[type], values[name], name);
      }, this)
      .filter(Boolean);

    if (value.length) {
      return value;
    }
  } else {
    value = values;
  }

  return this.type(value) === 'string' || this.type(value) === 'number' ? value : null;
};

//
// Get the proper hint code from the internal cassandra driver to pass in
//
Schema.prototype._mapFieldHint = function (hint) {
  var hintType = dataTypes[hint] ? dataTypes[hint] : hint;
  return this.type(hintType) === 'string'
    ? dataTypes.getByName(hintType)
    : hintType;
};

Schema.prototype._getFieldHint = function (field) {
  var meta = this.meta[field];
  var cType;

  if (!meta || !this._isString(meta.type)) return null;

  //
  // Validate and return hints for various types
  //
  if (meta.type === 'map') {
    return Array.isArray(meta.mapType)
    && meta.mapType.length === 2
    && meta.mapType.every(this._isString, this)
      ? util.format('map<%s,%s>', meta.mapType[0], meta.mapType[1])
      : null;
  }

  //
  // Handle set and lists which are formatted the same
  //
  if (['set', 'list'].indexOf(meta.type) !== -1) {
    cType = meta[meta.type + 'Type'];
    return this._isString(cType)
      ? util.format('%s<%s>', meta.type, cType)
      : null;
  }

  return meta.type;
};

//
// Helper function for the above
//
Schema.prototype._isString = function (type) {
  return this.type(type) === 'string';
};

/*
 *
 * Performs any coercion for types that have
 * different C* representations in range queries.
 */
function convertRangeType(converter, value, name) {
  return converter
    ? converter[name](value)
    : value;
}

/**
 *
 * Return an object from a given array with values set to true for a simple
 * lookup table
 * @param {Object} set - Set object
 * @returns {Object} - Returns the reduced value
 */
function createLookup(set) {
  return set.reduce(function (acc, key) {
    acc[key] = true;
    return acc;
  }, {});
}

/**
 * Simple function used to get the correct timeuuids for rangeQueries
 *
 * @param {Object} type - type of the time
 * @returns {TimeUuid} timeUuid - TimeUuid object
 */
function time(type) {
  return function (timeuuid) {
    var precision = TimeUuid.fromString(timeuuid).getDatePrecision();
    return TimeUuid[type](precision.date, precision.ticks);
  };
}

//
// Function used to default values based
//
function defaultValue(type) {
  return function () {
    var value;
    switch (type) {
      case 'uuid_v4':
        value = uuid();
        break;
      case 'uuid_empty':
        value = '00000000-0000-0000-0000-000000000000';
        break;
      case 'date_now':
        value = new Date();
        break;
      default :
        break;
    }
    return value;
  };
}
