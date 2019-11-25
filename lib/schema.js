const
  util      = require('util'),
  uuid      = require('uuid'),
  clone     = require('clone'),
  assign    = require('object-assign'),
  priam     = require('priam'),
  joi       = require('joi-of-cql'),
  snakeCase = require('./snake-case'),
  camelCase = require('./camel-case');

const
  dataTypes = priam.dataTypes,
  TimeUuid  = priam.valueTypes.TimeUuid;

const invalidChar = /\W/;

class Schema {

  /**
   * In this method we are going to create a denormalized structure from the
   * schema representation that we type out as JSON. This structure should be easy
   * to ask questions to and lookup various properties, etc.
   *
   * @param {string} name - Schema name
   * @param {Object} schema - Schema object
   * @param {String} schema.name - name of the table
   * @param {Array} schema.keys - Primary and secondary keys of a schema
   * @param {Object} schema.columns - An object of column names to their type
   * @param {Object} schema.maps - An object of the column names that are a special map type with their type as value
   * @param {Object} schema.sets - An object of the column names that are a special set type with their type as value
   * @constructor
   */
  constructor(name, schema) {
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
      }, {});

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
    let keys = schema.clusteringKey();
    if (!Array.isArray(keys)) {
      keys = [keys];
    }

    const pKey = schema.partitionKey();
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
        const newKey = this._aliases[key];
        acc[newKey] = key;
        return acc;
      }.bind(this), {});

    //
    // If lookupKeys exist in the schema, setup the proper properties to handle
    // those cases
    //
    const badLookups = this.setLookupKeys(schema.lookupKeys());

    if (badLookups) {
      throw badLookups;
    }
  }

  //
  // Require some set of keys to generate another joi schema
  //
  requireKeys(keys) {
    return keys.reduce((memo, key) => {
      if (!this.meta[key] || !this.meta[key].default) {
        memo[key] = joi.any().required();
      }
      return memo;
    }, {});
  }

  //
  // Create a separate Lookup table JUST for Lookup tables. Yes confusing I know,
  // Object lookup for actual cassandra lookup tables. This should default to
  // lookupKeys/lookupTables if it is an object
  //
  setLookupKeys(lookupKeys) {
    //
    // Return an error to be thrown if we are a compositePrimary key and we are
    // given lookupKeys as that is something we do not support
    //
    if (this.compositePrimary && lookupKeys &&
      (this.type(lookupKeys) === 'array' && lookupKeys.length !== 0)
      || (this.type(lookupKeys) === 'object' && Object.keys(lookupKeys).length !== 0)
    )
      throw new Error('You cannot create a lookup table with a compound key');

    lookupKeys = this.fixKeys(lookupKeys) || {};
    this.lookupTables = this.type(lookupKeys) === 'object'
      ? lookupKeys
      : lookupKeys.reduce((acc, key) => {
        acc[key] = this.name + '_by_' + key;
        return acc;
      }, {});

    lookupKeys = Object.keys(this.lookupTables);

    //
    // If there are any lookup keys that do not exist on this
    // Schema then return an error accordingly
    //
    const missingLookupKeys = lookupKeys.filter(key => {
      return !this.exists(key);
    });

    if (missingLookupKeys.length) {
      throw new Error('Invalid lookup keys: ' + missingLookupKeys.join(', '));
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
      .reduce((acc, key) => {
        const table = this.lookupTables[key];
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
  }

  //
  // Validate and default things
  //
  validate(entity, type) {
    type = type || 'update';
    const { error, value } = joi.validate(entity, this.validator[type], { context: { operation: type }});
    if (error) {
      throw error;
    } else {
      return value;
    }
  }

  //
  // Test if the key exists and returns the transformed key to use if it does,
  // otherwise returns undefined. This requires us
  // to transform the key to snake_case as well as remap any aliases so we can
  // specify the key as a standard camelCase key when passing in any options.
  //
  exists(key) {
    const transformed = this.fixKeys(key);

    return !this.meta[transformed] ? null : transformed;
  }

  //
  // Transform an entity key to the proper key that cassandra expects (snake_case, unalias)
  //
  entityKeyToColumnName(key) {
    const mappedKey = snakeCase(key);
    const alias = this._aliases[mappedKey];
    return alias || mappedKey;
  }

  //
  // Transform an entity, an object of conditions or an array of fields to have the proper
  // keys that cassandra expects (snake_case, unalias)
  //
  fixKeys(entity) {
    entity = entity || {};

    if (entity.isDatastar) {
      entity = entity.attributes.data;
    }

    if (this.type(entity) === 'object') {
      return Object.keys(entity).reduce((acc, key) => {
        //
        // If we have an alias, check it and convert it to what we expect in C*
        //
        const mappedKey = this.entityKeyToColumnName(key);
        acc[mappedKey] = entity[key];

        return acc;
      }, {});
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
      const mapped = snakeCase(entity);
      return this._aliases[mapped]
        ? this._aliases[mapped]
        : mapped || entity;
    }

    //
    // If we meet 0 conditions we just return what we got, this maybe should be an
    // error? Idk, this is just a weird thing in general
    //
    return entity;
  }

  //
  // Transform in the opposite direction of transform by remapping snakeCase back
  // to camelCase
  //
  toCamelCase(entity) {
    entity = entity || {};

    if (this.type(entity) === 'object') {
      return Object.keys(entity).reduce((acc, key) => {
        //
        // If we have an alias, check it and convert it to what we
        const mappedKey = camelCase(this._aliasesReverse[key] || key);

        acc[mappedKey] = entity[key];

        return acc;
      }, {});
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
  }

  //
  // Generate a conditions object given a value assumed to be the primary key
  //
  generateConditions(value) {
    const primaries = this.primaryKeys();

    if (primaries.length > 1) {
      throw new Error(util.format('More conditions required %s', primaries.join(', ')));
    }

    //
    // Return an object with the single primaryKey with the correct case assigned
    // to the value passed in. Allows us to support passing a string for findOne
    //
    return primaries.reduce((acc, key) => {
      acc[this.toCamelCase(key)] = value;
      return acc;
    }, {});
  }

  //
  // Return both primary and secondary keys
  //
  keys() {
    return this._keys;
  }

  //
  // Returns whether or not it is a primary or secondary key
  //
  isKey(key) {
    return !!this._keysLookup[key];
  }

  //
  // Return the column type for the given
  //
  fieldMeta(field) {
    return this.meta[field];
  }

  prepareForUse(data) {
    return this.convert(this.fixKeys(data), 'deserialize');
  }

  // unknown use case
  prepareForSerialization(data) {
    return this.convert(this.fixKeys(data), 'serialize');
  }

  convert(data, converter) {
    const meta = this.meta;
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
  }

  //
  // Return the primaryKey based on what type it is which is probably an array.
  // Handle the other case as well
  //
  primaryKeys() {
    return Array.isArray(this._primaryKeys) && this._primaryKeys.length
      ? this._primaryKeys
      : [this._primaryKeys];
  }

  secondaryKeys() {
    return this._secondaryKeys;
  }

  fields() {
    return this._columnKeys;
  }

  fieldString(fieldList) {
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
  mappedFields() {
    if (!this._mappedFields) {
      this._mappedFields = this._columnKeys.map(key => {
        //
        // CamelCase and replace alias with actual key name if it exists
        //
        return camelCase(this._aliasesReverse[key] || key);
      });
    }

    return this._mappedFields;
  }

  //
  // Appropriate typeof checking
  //
  type(of) {
    return Object.prototype.toString.call(of).slice(8, -1).toLowerCase();
  }

  //
  // Remark: Create conditions that are meant to be directed at the primary table if there
  // is a lookup table situtation. We filter based on the remove logic and do not
  // parse into conditionals as this gets passed directly to find
  //
  filterPrimaryConditions(conditions) {
    return this.toCamelCase(this.filterRemoveConditions(this.fixKeys(conditions)));
  }

  //
  // Evaluate if we have sufficient conditions for the remove we are executing and
  // return them
  //
  createRemoveConditions(conditions, table) {
    const transformed = this.fixKeys(conditions);
    //
    // If we are a lookup table and insufficient conditions are passed to execute
    // the queries to ALL the lookup tables, just error for simplicity now.
    //
    if (!this.sufficientRemoveConditions(transformed)) {
      throw new Error('Must pass in all primary keys when using lookup tables');
    }

    conditions = this.filterRemoveConditions(transformed, table);

    const conditionals = this.parseConditions(conditions);
    conditionals.table = table;

    return conditionals;
  }

  //
  // Evaluate if we have sufficient conditions for the remove we are executing and
  // return them
  //
  createUpdateConditions(conditions, table) {
    const transformed = this.fixKeys(conditions);
    //
    // If we are a lookup table and insufficient conditions are passed to execute
    // the queries to ALL the lookup tables, just error for simplicity now. Also
    // handle the case where we do not have sufficient keys for a query, (need all
    // primary keys or both secondary and primary)
    //
    if (!this.sufficientUpdateConditions(transformed)) {
      throw new Error(util.format('All necessary primary keys must be passed in, given: %j', conditions));
    }

    conditions = this.filterRemoveConditions(transformed, table);

    const conditionals = this.parseConditions(conditions);
    conditionals.table = table;

    return conditionals;
  }

  //
  // Ensure we have sufficient keys to do an update operation
  //
  sufficientUpdateConditions(conditions) {
    const keys = this.lookups ? this.keys().concat(Object.keys(this.lookupTables)) : this.keys();
    return keys.every(function (key) {
      return !!conditions[key];
    });
  }

  //
  // DE-Null the entity, meaning translate known types into our defined null
  // equivalents. We expect to receive a fully transformed object with snake case
  // keys here. We use a for loop since we do too many iterations over the object
  // in this process
  //
  deNull(entity) {
    const keys = Object.keys(entity);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const value = entity[key];
      const meta = this.fieldMeta(key);
      if (!meta) {
        throw new Error(util.format('%s is not found in the schema', key));
      }

      entity[key] = this.nullToValue(meta, value);
    }

    return entity;
  }

  hasAllRequiredKeys(entity, previous) {
    if (!entity) {
      return false;
    }

    try {
      this.validate(assign(clone(entity), previous || {}), 'update');
      return true;
    } catch (err) {
      return false;
    }
  }

  //
  // Adjust detected values that are `null` and map them to a `null-like` value.
  // TODO: Should we iterate through maps and sets and adjust accordingly as well?
  //
  nullToValue(meta, value) {
    const type = meta.type;

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
      return Object.keys(value).reduce((memo, key) => {
        memo[key] = this.nullToValue({ type: meta.mapType[1] }, value[key]);
        return memo;
      }, {});
    }
    if (type === 'set') {
      // Sets are an odd edge case here, it can be an array or an object who's
      // values are sit in an add and/or remove property. This means we need to
      // a bit more work updating this data structure.
      if (this.type(value) === 'object') {
        ['add', 'remove'].forEach(method => {
          if (method in value) value[method] = value[method].map(value => {
            return this.nullToValue({ type: meta.setType }, value);
          });
        });

        return value;
      }
      return value.map(value => {
        return this.nullToValue({ type: meta.setType }, value);
      });

    }
    if (type === 'list') {
      if (this.type(value) === 'object') {
        ['prepend', 'append', 'remove'].forEach(method => {
          if (method in value) value[method] = value[method].map(value => {
            return this.nullToValue({ type: meta.listType }, value);
          });
        });

        if (value.index && this.type(value.index) === 'object') {
          value.index = Object.keys(value.index).reduce((acc, idx) => {
            acc[idx] = this.nullToValue({ type: meta.listType }, value.index[idx]);
            return acc;
          }, {});
        }
      } else {
        return value.map(value => {
          return this.nullToValue({ type: meta.setType }, value);
        });
      }
    }

    return value;
  }

  //
  // RE-Null the entity. This translates the defined null equivalents
  // into an actual null value for the consumer to use.
  //
  reNull(entity) {
    const keys = Object.keys(entity);

    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (this.requiresNullConversion(key)) {
        entity[key] = this.valueToNull(entity[key]);
      } else if (!this.isKey(key)) {
        entity[key] = this.nullToValue(this.fieldMeta(key), entity[key]);
      }
    }

    return entity;
  }

  requiresNullConversion(columnName) {
    if (!this.meta[columnName]) {
      columnName =  this.entityKeyToColumnName(columnName);
    }
    const metaData = this.fieldMeta(columnName);
    const isKey = this.isKey(columnName);
    if (isKey) { return false; }
    if (!metaData || typeof metaData.nullConversion !== 'boolean') { return true; }
    return metaData.nullConversion;
  }
  
  //
  // Detect our `null-like` values and return null if applicable
  //
  valueToNull(value) {
    return valueToNullImpl(value, this.type.bind(this), new WeakSet());
  }

  //
  // Assess if we have sufficient conditions during our pre-remove check from
  // a table with a lookup table. This will let our user know if they are trying
  // to do something they can't do based on how they defined lookup tables
  //
  sufficientRemoveConditions(conditions) {
    const keys = this.lookups
      ? Object.keys(this.lookupTables).concat(this.primaryKeys())
      : this.primaryKeys();

    return keys.every(function (key) {
      return !!conditions[key];
    });
  }

  //
  //
  // These are conditions specific for the remove functionality in the case where
  // we are removing from a bunch of lookup tables. Also handles the generic case
  //
  filterRemoveConditions(conditions, table) {
    //
    // Filter the conditions and pluck the appropriate primary key and secondary
    // keys based on the table
    //
    return Object.keys(conditions)
      .filter(key => {
        //
        // Only allow secondary keys or the appropriate primary key. If a table is
        // passed, we check the lookup table keys as well
        //
        return (table
          ? this._reverseLookupKeyMap[table] === key
          : this._primaryKeysLookup[key])
          || this._secondaryKeysLookup[key];
      })
      .reduce(function (acc, key) {
        acc[key] = conditions[key];
        return acc;
      }, {});
  }

  //
  // Remark: Transform the keys and then filter out any keys that are not the
  // primary/secondary keys that are used as conditions to query on (creating
  // the where clause)
  //
  filterConditions(conditions) {
    let table;
    const primaries = [];

    const filtered = Object.keys(conditions)
      .filter(key => {
        //
        // If it exists as a primary or secondary key, we keep it and dont filter
        //
        const exists = !!this._keysLookup[key];
        if (this._primaryKeysLookup[key]) primaries.push(key);
        //
        // Check if its part of a lookup table
        //
        table = this.lookupTables[key];
        if (table) primaries.push(key);

        return exists || !!table;
      })
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
      throw new Error('There can only be 1 primary key in a query, found ' + primaries.length + ' ' + primaries);
    }

    return { table: table, conditions: filtered };
  }

  //
  // Create conditions based on an entity or conditions. Optional type paremeter
  // can be passed as there is one case we don't want lookup table primary keys to be
  // considered valid conditions (remove);
  //
  createConditions(conditions) {
    const opts = this.filterConditions(this.fixKeys(conditions));
    const conditionals = this.parseConditions(opts.conditions);
    //
    // Pass back the table so we can override the standard table after we have
    // parsed the conditions
    //
    conditionals.table = opts.table;
    return conditionals;
  }

  //
  // Parse the conditions into array objects to be used later on
  //
  parseConditions(conditions) {
    //
    // Create a data structure
    //
    const conditionals = {
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
    Object.keys(conditions).forEach(field => {
      const value = conditions[field];
      conditionals.fields.push(field);
      conditionals.query.push(this._getQuery(field, value));

      //
      // Do valueOf on the params to get the value expected by priam.
      // Whats returned by this._getParams is actually the proper value for the
      // query
      //
      let params = this._getParams(field, value);
      params = Array.isArray(params) ? params : [params];
      params.forEach(function (param) {
        conditionals.params.push(this.valueOf(field, param));
      }, this);

    });

    return conditionals;
  }

  //
  // Return the params based on the given entity
  //
  getValues(entity, fields) {
    fields = fields || this.fields();

    //
    // Populate all fields (i.e. columns) with
    // any values from the entity. If a value for
    // a particular column is not present we set
    // it EXPLICITLY to `null`.
    //
    return fields.map(field => {
      let value = null;
      if (entity.hasOwnProperty(field)) {
        value = entity[field];
      }

      return this.valueOf(field, value);
    });
  }

  //
  // Bit of a hack that returns the data structure expected by priam
  //
  valueOf(field, value, type) {
    return {
      value: value,
      hint: this._mapFieldHint(
        type ? type : this._getFieldHint(field)
      ),
      isRoutingKey: this.primaryKeys().indexOf(field) !== -1
    };
  }

  //
  // Add the column names and aliases from the schema definition as
  // property getters/setters for the data being modeled by this object
  //
  buildProperties() {
    const columns = Object.keys(this.meta);
    const aliasesOf = this._aliasesReverse;

    const definitions = columns.reduce(function (memo, name) {
      name = camelCase(aliasesOf[name] || name);
      memo[name] = {
        get() {
          return this.attributes.get(name);
        },
        set(value) {
          return this.attributes.set(name, value);
        },
        enumerable: true,
        configurable: true
      };
      return memo;
    }, {});

    return definitions;
  }

  //
  //
  _getQuery(field, values) {
    let value;
    if (Array.isArray(values)) {
      if (values.length > 1) {
        return util.format('%s IN (%s)', field, '?' + new Array(values.length).join(', ?'));
      }
      value = values[0];
    } else if (this.type(values) === 'object') {
      value = Object.keys(values)
        .map(name => {
          const op = this.operators[name];

          return op
            ? util.format('%s %s ?', field, op)
            : null;
        })
        .filter(Boolean)
        .join(' AND ');

      return value || null;
    } else {
      value = values;
    }

    return this.type(value) === 'string' || this.type(value) === 'number'
      ? util.format('%s = ?', field)
      : null;
  }

  //
  // Transform parameters based on the field passed in and the value associated
  // with the field
  //
  _getParams(field, values) {
    let value;

    if (Array.isArray(values)) {
      values = values.slice(0);
      if (values.length > 1) {
        return values;
      }
      value = values[0];
    } else if (this.type(values) === 'object') {
      value = Object.keys(values)
        .map(function (name) {
          const op = this.operators[name];
          if (!op) {
            return null;
          }

          const type = this.meta[field].type;
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
  }

  //
  // Get the proper hint code from the internal cassandra driver to pass in
  //
  _mapFieldHint(hint) {
    const hintType = dataTypes[hint] ? dataTypes[hint] : hint;
    return this.type(hintType) === 'string'
      ? dataTypes.getByName(hintType)
      : hintType;
  }

  _getFieldHint(field) {
    const meta = this.meta[field];
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
      const cType = meta[meta.type + 'Type'];
      return this._isString(cType)
        ? util.format('%s<%s>', meta.type, cType)
        : null;
    }
  
    return meta.type;
  }
  
  //
  // Helper function for the above
  //
  _isString(type) {
    return this.type(type) === 'string';
  };

}

//
// detect both empty string and null as a bad uuid value since cassandra will
// give us weird errors if we try and insert an empty string
//
function isBadUuid(value) {
  return value === null || (typeof value === 'string' && value.length === 0);
}

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
    const precision = TimeUuid.fromString(timeuuid).getDatePrecision();
    return TimeUuid[type](precision.date, precision.ticks);
  };
}

//
// Function used to default values based
//
function defaultValue(type) {
  return function () {
    let value;
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
      default:
        break;
    }
    return value;
  };
}


//
// Detect our `null-like` values and return null if applicable.
// Implements recursion for `valueToNull` prototype function.
//
function valueToNullImpl(value, getType, visited) {
  if (value === '\x00') {
    return null;
  }
  if (value === '00000000-0000-0000-0000-000000000000') {
    return null;
  }

  const type = getType(value);

  if (type === 'date' && value.getTime() === 0) {
    return null;
  }

  if (value === null || typeof value === 'undefined') {
    return value;
  }

  if (isObject(value)) {
    // Prevent cyclic structures from being re-evaluated
    if (visited.has(value)) {
      return value;
    }
    visited.add(value);

    if (type === 'array') {
      for (let i = 0; i < value.length; i++) {
        const arrValue = value[i];
        if (!isObject(arrValue) || !visited.has(arrValue)) {
          value[i] = valueToNullImpl(arrValue, getType, visited);
        }
      }
    } else if (type === 'object') {
      const keys = Object.keys(value);
      for (let i = 0; i < keys.length; i++) {
        const keyValue = value[keys[i]];
        if (!isObject(keyValue) || !visited.has(keyValue)) {
          value[keys[i]] = valueToNullImpl(keyValue, getType, visited);
        }
      }
    }
  }

  return value;
}

function isObject(value) {
  return typeof value === 'object' && value !== null;
}

module.exports = Schema;
