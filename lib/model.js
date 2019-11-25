const
  { EventEmitter }            = require('events'),
  { PassThrough, Transform }  = require('stream'),
  { promisify }               = require('util'),
  once                        = require('one-time'),
  pick                        = require('lodash.pick'),
  async                       = require('async'),
  clone                       = require('clone'),
  assign                      = require('object-assign'),
  ls                          = require('list-stream'),
  camelCase                   = require('./camel-case'),
  Schema                      = require('./schema'),
  StatementBuilder            = require('./statement-builder'),
  StatementCollection         = require('./statement-collection'),
  Attributes                  = require('./attributes');

//
// Types that return a single value from cassandra
//
const singleTypes = ['count', 'one', 'first'];

/*
 * Constructor function for the base Model which handles
 * all of the base CRUD logic for a given connection.
 *
 * Reserve constructor for prototype based initialization.
 * We use the functions that exist on the constructor in our specific prototype
 * functions that we define
 */
const Model = module.exports = function Model() {
};

Model.init = function init(options) {
  this.options = options || {};

  // NOTE: For testing:
  if (options.perform) {
    this.perform = options.perform;
  }

  this.schema = new Schema(options.name, options.schema);
  this.builder = new StatementBuilder(this.schema);

  Object.defineProperties(this.prototype, this.schema.buildProperties());

  //
  // Should we have better defaults?
  //
  this.readConsistency = options.readConsistency || options.consistency || 'one';
  this.writeConsistency = options.writeConsistency || options.consistency || 'one';

  //
  // Remark Add an event emitter to the model constructor we are creating
  //
  this.emitter = new EventEmitter();

  //
  // Expose the EventEmitter on the constructor to proxy from our real event emitter
  //
  Object.keys(EventEmitter.prototype).forEach(k => {
    this[k] = (...args) => {
      return this.emitter[k].apply(this.emitter, args);
    };
  });

  //
  // Remark: We might not need to do the following, but we should look into the
  // dangers of what happens when a primary/secondary key is modified
  // Before we update we need to check a few things.
  // 1. If we have lookup tables, we need to ensure we have a `previous` entity
  // with all the primary keys
  // 2. If any `previous` value(s) are provided ensure that they have the necessary
  // keys for all lookup tables. If a `previous` value is not provided, execute
  // a find using the main primary key before continuing the update.)
  // 3. So for this to happen, we must
  //    i. Ensure we have the main primaryKey in the entity given to us
  //    ii. Do a find query on the primary table (table that uses the
  //    main primary key) to fetch the previous value
  //    iii. We have to detect if other lookup tables primary keys changed, if
  //    so, pass in a special truthy flag for the updateStatement to pick up on
  //
  this.before('update:build', (updateOptions, callback) => {
    const entities = updateOptions.entities;
    const previous = updateOptions.previous;

    //
    // If we aren't dealing with lookup tables or we already have a previous
    // value being passed in, we dont have to do anything crazy
    //
    if (!this.schema.lookups || entities.length === previous.length) {
      return callback();
    }
    //
    // This case just cant happen
    //
    if (entities.length !== previous.length && previous.length !== 0) {
      return void callback(
        new Error('You must pass in the same number of `entities` as `previous` values for an update on multiple entities'));
    }
    //
    // If we are dealing with lookup tables and we do not have a previous value,
    // we need to fetch it.
    //
    async.map(entities, (entity, next) => {
      this.findOne({
        conditions: this.schema.filterPrimaryConditions(clone(entity))
      }, next);
    }, function (err, previous) {
      if (err) return void callback(err);
      updateOptions.previous = previous;
      callback();
    });

  });

  if (this.options.ensureTables) {
    this.ensureTables(err => {
      if (err) {
        return this.emit('error', err);
      }
      this.emit('ensure-tables:finish', this.schema);
    });
  }

  this.waterfallAsync = promisify(this.waterfall.bind(this));
};

/**
 * Execute an action on a model
 *
 * @param {Object} Options object containing previous statements, entity, etc.
 * @param {Function} Continuation callback to respond when finished
 */
['create', 'update', 'remove'].forEach(function (action) {
  Model[action] = function (options, callback) {
    options = this.validateArgs(options, callback);
    if (!options) {
      return;
    }

    const statements = options.statements = options.statements
      || (new StatementCollection(this.connection, options.strategy)
        .consistency(options.consistency || this.writeConsistency));

    //
    // Add a hook before the statement is created
    //
    this.perform(action + ':build', options, next => {
      try {
        //
        // We should keep this naming generic so we can remove all this
        // boilerplate in the future.
        //
        const entities = options.entities;
        //
        // Remark: Certain cases this is the previous entity that could be used
        // for update. its required for lookup-table update, otherwise we fetch it
        //
        const previous = options.previous;

        for (let e = 0; e < entities.length; e++) {
          // shallow clone
          const opts = assign({}, options);

          opts.previous = previous && previous.length
            ? options.previous[e]
            : null;

          const statement = this.builder[action](opts, entities[e]);

          statements.add(statement);
        }

        //
        // If there was an error building the statement, return early
        //
        return void next();
      } catch (err) {
        setImmediate(next, err)
      }
    }, err => {
      if (err) {
        return void callback(err);
      } else if (!options.shouldExecute) {
        return void callback(null, statements);
      }

      //
      // Add a hook before the statements are executed
      //
      this.perform(action + ':execute', options, function (next) {
        statements.execute(next);
      }, callback);
    });
  };
});

/*
 * Performs the base find logic for any given schema
 * and connection.
 */
/* eslint consistent-return: 0 */
Model.find = function find(options, callback) {
  try {
    options = this.validateFind(options || {}, callback);
  } catch (err) {
    return callback ? void callback(err) : errorStream(err);
  }

  options.type = options.type || 'all';
  const action = options.type !== 'all'
    ? 'find:' + options.type
    : 'find';

  //
  // If we are streaming, we need a proxy stream
  //
  const proxy = options.stream && new PassThrough({ objectMode: true });

  //
  // This allows for cascading of after functions to mutate the result of this
  // perform method
  //
  const promise = this.waterfallAsync(action, options, next => {
    //
    // Ensure next can only be called once
    // If we are not a stream, figure out if we want to return an object or an
    // array, and return the appropriate thing by unwrapping if necessary
    //
    const fn = once(function (err, result) {
      if (singleTypes.indexOf(options.type) !== -1) result = result && result[0];
      next(err, result);
    });

    try {
      this.builder.find(options)
        //
        // ExtendQuery returns the priam connection query so we have access to
        // those functions
        //
        .extendQuery(this.connection.beginQuery())
        //
        // Allow configurable consistency
        //
        .consistency(options.consistency || this.readConsistency)
        .stream()
        .on('error', fn)
        //
        // Simple Stream to re-transform back to camelCase keys
        //
        .pipe(new Transform({
          writableObjectMode: true,
          readableObjectMode: true,
          transform: (data, _, callback) => {
            return void callback(null, this.toInstance(data));
          }
        }))
        //
        // Pipe the stream to the proxy stream or the list-stream that will collect
        // the data for us and return to the caller
        //
        .pipe(proxy ? proxy : ls.obj(fn));
    } catch (err) {
      return void fn(err);
    }
  });

  promise.then(result => {
    if (!proxy) {
      return void callback(null, result);
    }
  }, err => {
    return proxy
      ? proxy.emit('error', err)
      : void callback(err);
  });

  //
  // We return the stream or undefined
  //
  return proxy || null;
};

/*
 * Create the table based on the schema if it doesn't already exist.
 */

['ensure', 'drop'].forEach(function (type) {
  const action = [type, 'tables'].join('-');
  const name = camelCase(action);

  Model[name] = function (options, callback) {
    if (!callback) {
      callback = options || function () {};
      options = this.options;
    }

    //
    // Simple validation of merged options
    //
    options = this.assessOpts(
      assign(
        pick(this.options, ['alter', 'orderBy', 'with']), options
      )
    );

    //
    // Set the type based on the prefix so we know what statement we are
    // generating
    //
    options.type = type;

    const statements = options.statements = options.statements
      || (new StatementCollection(this.connection, options.strategy));

    this.perform([action, 'build'].join(':'), options, next => {
      try {
        const statement = this.builder.table(options);
        statements.add(statement);
  
        if (!this.schema.lookups) {
          return void setImmediate(next);
        }
  
        Object.keys(this.schema.lookupTables).forEach(primaryKey => {
          // shallow clone
          const tableOpts = assign({}, options, { lookupKey: primaryKey });
          const lookupStatement = this.builder.table(tableOpts);
          statements.add(lookupStatement);
        });
  
        next();  
      } catch (err) {
        setImmediate(next, err);
      }
    }, err => {
      if (err) {
        return void callback(err);
      } else if (!options.shouldExecute) {
        return void callback(null, statements);
      }

      this.perform([action, 'execute'].join(':'), options, function (next) {
        statements.execute(next);
      }, callback);
    });
  };
});


/*
 * Default `perform` method.
 */
Model.perform = function perform(action, options, callback) {
  return setImmediate(callback, function () {
  });
};

/*
 * Define the wrapper methods for:
 *
 *   - findFirst
 *   - findOne
 *   - count
 *
 * These methods simple wrap `Model.find` but set the
 * appropriate option for the corresponding method
 * before execution.
 */
const findTypes = {
  findFirst: 'first',
  findOne: 'one',
  count: 'count',
  findAll: 'all'
};

const findTypesLookup = Object.keys(findTypes).reduce(function (acc, key) {
  acc[findTypes[key]] = true;
  return acc;
}, {});

Object.keys(findTypes).forEach(function (method) {
  Model[method] = function (options, callback) {
    options = options || {};
    const type = findTypes[method];
    //
    // Remark: If we are a type of function that returns a single value from cassandra
    // check to see if a simplified options object was passed
    //
    options = this.normalizeFindOpts(options, callback);
    if (!options) return;
    //
    // Assign type and pass it down
    //
    options.type = type;
    return this.find(options, callback);
  };
});

/**
 * Alias get and findOne for simpler syntax
 */
Model.get = Model.findOne;

//
// Returns a stream that emits an error that it is given. Useful for validation
// errors
//
function errorStream(error) {
  const stream = new PassThrough({ objectMode: true });
  setImmediate(stream.emit.bind(stream), 'error', error);
  return stream;
}

//
// Simple validation for ensureTables so that we know when to execute
//
Model.assessOpts = function assessOpts(options) {
  const opts = assign({}, options);

  //
  // Ensure we are not executed as a batch because you cant do that when we are
  // doing a TABLE based operation
  // TODO: Get a list of operations that cant be involved in a batch
  //
  opts.strategy = 10;

  if (!opts.statements) {
    opts.shouldExecute = true;
  }

  return opts;
};


/**
 *
 * Allow a simpler API syntax that gets normalized into the conditions object
 * expected by the rest of the module
 * @param {Object} options - Options object
 * @param {Function} callback - Callback function
 * @returns {*} - Return varies on the different usecases of the function
 */
Model.normalizeFindOpts = function (options, callback) {
  const opts = {};

  //
  // We assume this is the primary/partition key. We also assume that this
  // function will only be run on find methods that return a single value and
  // a callback is always passed which is a safe assumption in reality
  //
  if (this.schema.type(options) === 'string') {
    try {
      opts.conditions = this.schema.generateConditions(options);
      return opts;  
    } catch (err) {
      return void callback(err);
    }
  }

  //
  // Allow the object to be defined as options when we arent explicitly passing
  // a type
  //
  if (!options.conditions) {
    opts.conditions = options;
    return opts;
  }

  return options;
};


/**
 *
 * Validate Find Queries options specifically
 * @param {Object} options - Options object
 * @param {Function} callback - Callback function
 * @returns {*} - Return varies on the different usecases of the function
 */
Model.validateFind = function validateFind(options, callback) {
  const stream = !callback || typeof callback !== 'function';
  const opts = assign({}, options);

  if (opts.type && !findTypesLookup[opts.type]) {
    throw new Error('Improper find type. Must be ' + Object.keys(findTypesLookup));
  }

  if (!opts.conditions) {
    throw new Error('Conditions must be passed to execute a find query');
  }
  //
  // Set this after we have transposed options -> entity for the simple case in
  // create
  //
  opts.stream = stream;

  return opts;
};
/*
 * Parses the options object into more comprehensible and consistent
 * options that are used internally. This is to help support older
 * API surface area with minimal spanning upgrade path.
 *
 * Remark: naming here is a little obtuse since `Args` is only
 * `Args` to CRUD methods: create, remove, update.
 *
 */
Model.validateArgs = function validateArgs(options, callback) {

  //
  // Argument Validation and normalization
  //
  if (!options) {
    return void setImmediate(callback, new Error('Options or entity must be passed in'));
  }
  //
  // Remark: Be a good citizen and use a copy internally just in case with
  // a shallow clone
  //
  let opts = options.isDatastar ? { entities: [options.toJSON(true)] } : assign({}, options);

  //
  // Lets assume an entity is given if there is no entity property for the
  // simple case.
  // Remark: We don't do this when a type is passed in (ie. find)
  //
  if (!opts.entity && !opts.entities) {
    const entity = opts;
    opts = { entities: !Array.isArray(entity) ? [entity] : entity };
  }
  //
  // Since we always map entity to be an array, do the same with previous since
  // we enable ourselves to handle arrays
  //
  if (opts.previous && !Array.isArray(opts.previous)) {
    opts.previous = [opts.previous];
  }

  //
  // Default to empty array
  //
  if (!opts.previous) {
    opts.previous = [];
  }

  //
  // Allow entity to be defined in the external interface for semantics but
  // remain consistent internally. We assume arrays since we are currently
  // handling arrays for less duplication
  //
  if (opts.entity && !opts.entities) {
    opts.entities = !Array.isArray(opts.entity)
      ? [opts.entity]
      : opts.entity;
  }

  if (opts.entity) {
    delete opts.entity;
  }


  if (!opts.statements) {
    opts.shouldExecute = true;
  }

  return opts;
};

Model.toInstance = function toInstance(data) {
  if (Array.isArray(data)) {
    return data.map(toInstance.bind(this));
  }
  return new this(data);
};

Model.prototype.init = function init(data) {
  this.attributes = new Attributes(this, data || {});
};

Model.prototype.isValid = function isValid(type) {
  try {
    this.validate(type);
    return true;
  } catch (err) {
    return false;
  }
};

Model.prototype.validate = function validate(type) {
  return this.Model.schema.validate(this.attributes.needsValidation(), type);
};

Model.prototype.toJSON = function toJSON(snake) {
  //
  // Work around JSON.stringify passing in the index of the array to this
  // function when an instance is a part of an array being stringified
  //
  if (typeof snake === 'string') {
    snake = false;
  }
  return this.attributes.toJSON(snake);
};

Model.prototype.save = function (fn) {
  if (!this.attributes.isDirty()) {
    return void setImmediate(fn);
  }

  try {
    const result = this.validate(this.id ? 'update' : 'create');
    return this.Model.update({
      entity: result,
      previous: this.attributes.previous()
    }, fn);
  } catch (err) {
    return void fn(err);
  }
};

Model.prototype.destroy = function (fn) {
  return this.Model.remove(this.toJSON(), fn);
};

Model.prototype.isDatastar = true;

