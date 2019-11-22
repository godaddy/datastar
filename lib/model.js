const
  { EventEmitter }    = require('events'),
  { PassThrough }     = require('stream'),
  once                = require('one-time'),
  pick                = require('lodash.pick'),
  async               = require('async'),
  clone               = require('clone'),
  assign              = require('object-assign'),
  through             = require('through2'),
  ls                  = require('list-stream'),
  camelCase           = require('./camel-case'),
  Schema              = require('./schema'),
  StatementBuilder    = require('./statement-builder'),
  StatementCollection = require('./statement-collection'),
  Attributes          = require('./attributes');

//
// Types that return a single value from cassandra
//
var singleTypes = ['count', 'one', 'first'];

/*
 * Constructor function for the base Model which handles
 * all of the base CRUD logic for a given connection.
 *
 * Reserve constructor for prototype based initialization.
 * We use the functions that exist on the constructor in our specific prototype
 * functions that we define
 */
var Model = module.exports = function Model() {
};

Model.init = function init(options) {
  var self = this;

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
  Object.keys(EventEmitter.prototype).forEach(function (k) {
    self[k] = function () {
      return self.emitter[k].apply(self.emitter, arguments);
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
  self.before('update:build', function buildUpdate(updateOptions, callback) {
    var entities = updateOptions.entities;
    var previous = updateOptions.previous;

    //
    // If we aren't dealing with lookup tables or we already have a previous
    // value being passed in, we dont have to do anything crazy
    //
    if (!self.schema.lookups || entities.length === previous.length) {
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
    async.map(entities, function (entity, next) {
      self.findOne({
        conditions: self.schema.filterPrimaryConditions(clone(entity))
      }, next);
    }, function (err, previous) {
      if (err) return void callback(err);
      updateOptions.previous = previous;
      callback();
    });

  });

  if (self.options.ensureTables) {
    self.ensureTables(function (err) {
      if (err) {
        return self.emit('error', err);
      }
      self.emit('ensure-tables:finish', self.schema);
    });
  }
};

/**
 * Execute an action on a model
 *
 * @param {Object} Options object containing previous statements, entity, etc.
 * @param {Function} Continuation callback to respond when finished
 */
['create', 'update', 'remove'].forEach(function (action) {
  Model[action] = function (options, callback) {
    var self = this;

    options = this.validateArgs(options, callback);
    if (!options) {
      return;
    }

    var statements = options.statements = options.statements
      || (new StatementCollection(this.connection, options.strategy)
        .consistency(options.consistency || this.writeConsistency));

    //
    // Add a hook before the statement is created
    //
    this.perform(action + ':build', options, function (next) {
      //
      // We should keep this naming generic so we can remove all this
      // boilerplate in the future.
      //
      var entities = options.entities;
      //
      // Remark: Certain cases this is the previous entity that could be used
      // for update. its required for lookup-table update, otherwise we fetch it
      //
      var previous = options.previous;

      var error;
      for (var e = 0; e < entities.length; e++) {
        // shallow clone
        var opts = assign({}, options);

        opts.previous = previous && previous.length
          ? options.previous[e]
          : null;

        var statement = self.builder[action](opts, entities[e]);

        //
        // TODO: Use something like `errs` to propagate back the error with the
        // actual entity object that was the culprit
        //
        if (statement instanceof Error) {
          error = statement;
          break;
        }

        statements.add(statement);
      }

      //
      // If there was an error building the statement, return early
      //
      return error
        ? void setImmediate(next, error)
        : void next();

    }, function (err) {
      if (err) {
        return void callback(err);
      } else if (!options.shouldExecute) {
        return void callback(null, statements);
      }

      //
      // Add a hook before the statements are executed
      //
      self.perform(action + ':execute', options, function (next) {
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
  options = options || {};
  var self = this;

  options = self.validateFind(options, callback);
  if (!options) return;
  //
  // Handle the case where we return a stream error
  //
  if (options instanceof Error) {
    return errorStream(options);
  }

  options.type = options.type || 'all';
  var action = options.type !== 'all'
    ? 'find:' + options.type
    : 'find';

  //
  // If we are streaming, we need a proxy stream
  //
  var proxy;

  //
  // Make a proxy stream for returning a stream
  //
  if (options.stream) {
    proxy = new PassThrough({ objectMode: true });
  }

  function done(err, result) {
    if (err) {
      return proxy
        ? proxy.emit('error', err)
        : callback(err);
    }


    if (!proxy) {
      return void callback(err, result);
    }
  }

  //
  // This allows for cascading of after functions to mutate the result of this
  // perform method
  //
  self.waterfall(action, options, function (next) {
    var statement = self.builder.find(options);
    //
    // Ensure next can only be called once
    // If we are not a stream, figure out if we want to return an object or an
    // array, and return the appropriate thing by unwrapping if necessary
    //
    var fn = once(function (err, result) {
      if (singleTypes.indexOf(options.type) !== -1) result = result && result[0];
      next(err, result);
    });
    //
    // TODO: Use something like `errs` to propagate back the error with the
    // actual entity object that was the culprit
    //
    if (statement instanceof Error) {
      return void setImmediate(next, statement);
    }

    var stream = statement
      //
      // ExtendQuery returns the priam connection query so we have access to
      // those functions
      //
      .extendQuery(self.connection.beginQuery())
      //
      // Allow configurable consistency
      //
      .consistency(options.consistency || self.readConsistency)
      .stream();

    //
    // Pipe the stream to the proxy stream or the list-stream that will collect
    // the data for us and return to the caller
    //
    stream
      .on('error', fn)
      //
      // Simple Stream to re-transform back to camelCase keys
      //
      .pipe(through.obj(function (data, enc, callback) {
        return void callback(null, self.toInstance(data));
      }))
      .pipe(proxy ? proxy : ls.obj(fn));


  }, done);

  //
  // We return the stream or undefined
  //
  return proxy || null;
};

/*
 * Create the table based on the schema if it doesn't already exist.
 */

['ensure', 'drop'].forEach(function (type) {
  var action = [type, 'tables'].join('-');
  var name = camelCase(action);

  Model[name] = function (options, callback) {
    var self = this;
    if (!callback) {
      callback = options || function () {};
      options = self.options;
    }

    //
    // Simple validation of merged options
    //
    options = self.assessOpts(
      assign(
        pick(self.options, ['alter', 'orderBy', 'with']), options
      )
    );

    //
    // Set the type based on the prefix so we know what statement we are
    // generating
    //
    options.type = type;

    var statements = options.statements = options.statements
      || (new StatementCollection(self.connection, options.strategy));

    self.perform([action, 'build'].join(':'), options, function (next) {
      var statement = self.builder.table(options);
      if (statement instanceof Error) {
        return void setImmediate(next, statement);
      }

      statements.add(statement);

      if (!self.schema.lookups) {
        return void setImmediate(next);
      }

      var error;
      Object.keys(self.schema.lookupTables).every(function (primaryKey) {
        // shallow clone
        var tableOpts = assign({}, options, { lookupKey: primaryKey });

        var lookupStatement = self.builder.table(tableOpts);
        if (lookupStatement instanceof Error) {
          error = lookupStatement;
          return false;
        }

        statements.add(lookupStatement);
        return true;
      }, self);

      return error ? setImmediate(next, error) : next();
    }, function (err) {
      if (err) {
        return void callback(err);
      } else if (!options.shouldExecute) {
        return void callback(null, statements);
      }

      self.perform([action, 'execute'].join(':'), options, function (next) {
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
var findTypes = {
  findFirst: 'first',
  findOne: 'one',
  count: 'count',
  findAll: 'all'
};

var findTypesLookup = Object.keys(findTypes).reduce(function (acc, key) {
  acc[findTypes[key]] = true;
  return acc;
}, {});

Object.keys(findTypes).forEach(function (method) {
  Model[method] = function (options, callback) {
    var type;
    options = options || {};
    type = findTypes[method];
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
  var stream = through.obj();
  setImmediate(stream.emit.bind(stream), 'error', error);
  return stream;
}

//
// Simple validation for ensureTables so that we know when to execute
//
Model.assessOpts = function assessOpts(options) {
  var opts = assign({}, options);

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
  var opts = {};

  //
  // We assume this is the primary/partition key. We also assume that this
  // function will only be run on find methods that return a single value and
  // a callback is always passed which is a safe assumption in reality
  //
  if (this.schema.type(options) === 'string') {
    opts.conditions = this.schema.generateConditions(options);
    return this.schema.type(opts.conditions) === 'error'
      ? void callback(opts.conditions)
      : opts;
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
  var stream = !callback || typeof callback !== 'function';

  function error(err) {
    if (!stream) return void setImmediate(callback, err);
    return err;
  }

  var opts = assign({}, options);

  if (opts.type && !findTypesLookup[opts.type]) {
    return error(new Error('Improper find type. Must be ' + Object.keys(findTypesLookup)));
  }

  if (!opts.conditions) {
    return error(new Error('Conditions must be passed to execute a find query'));
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
  var opts = options.isDatastar ? { entities: [options.toJSON(true)] } : assign({}, options);

  //
  // Lets assume an entity is given if there is no entity property for the
  // simple case.
  // Remark: We don't do this when a type is passed in (ie. find)
  //
  if (!opts.entity && !opts.entities) {
    var entity = opts;
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
  return this.validate(type) instanceof Error;
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

  var result = this.validate(this.id ? 'update' : 'create');

  if (result instanceof Error) {
    return void fn(result);
  }

  return this.Model.update({
    entity: result,
    previous: this.attributes.previous()
  }, fn);
};

Model.prototype.destroy = function (fn) {
  return this.Model.remove(this.toJSON(), fn);
};

Model.prototype.isDatastar = true;

