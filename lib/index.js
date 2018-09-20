/* eslint no-proto:0 */


var Priam      = require('priam'),
  Understudy = require('understudy'),
  Model      = require('./model'),
  jtc        = require('joi-of-cql'),
  AwaitWrap       = require('./await-wrap');

/**
 * Constructor function for the Datastar object which is responsible
 * for defining a set of models associated with a given connection
 * defined in the `connect` function provided.
 *
 * @constructor
 * @type {module.Datastar}
 * @param {Object} - options
 * @param {function} - connect
 */

var Datastar = module.exports = function Datastar(options, connect) {
  this.options = options || {};
  this.connect = connect || this.connect;
};

//
// Alias to joi for defining schema for a model
//
Datastar.prototype.schema = jtc;

//
// Expose StatementCollection on the datastar instance
//
Datastar.prototype.StatementCollection = require('./statement-collection');

/**
 * Attach the connection to the model constructor
 * @param {Object} objectModel - Defined object model
 * @returns {Datastar} - The datastar object with the attached object model
 */
Datastar.prototype.attach = function attach(objectModel) {
  if (!this.connection) {
    this.connect();
  }

  objectModel.connection = this.connection;

  return this;
};

/**
 * Default connection logic which works with `priam`. This abstract
 * exists for future extensibility.
 * @param {Function} callback - The async callback
 * @returns {Datastar} - The datastar object
 */
Datastar.prototype.connect = function connect(callback) {
  var config = this.options.config;
  //
  // Use the cached connection if a model has been defined already via `attach`
  // so we dont create more than 1 priam instance. This allows `connect` to be
  // called with the callback to ensure the connection is pre-heated for all
  // models
  //
  this.connection = this.connection || new Priam(this.options);

  var create = config && config.keyspaceOptions &&
      'CREATE KEYSPACE IF NOT EXISTS ' + config.keyspace +
      ' WITH replication = ' +
      JSON.stringify(config.keyspaceOptions).replace(/"/g, "'") +
      ';';

  if (create) {
    //
    // Try to create the keyspace. As a side effect pre-heat the connection
    //
    this.connection.cql(create, [], { keyspace: 'system' }, callback);
  } else if (callback) {
    //
    // If a callback is passed, we pre-heat the connection
    //
    this.connection.connect(callback);
  }
  return this;
};


/**
 * Close the underlying connection
 *
 * @param {Function} callback - The async callback
 * @returns {Datastar} - The datastar object
 */
Datastar.prototype.close = function (callback) {
  this.connection.close(callback);
  return this;
};

/*
 * Defines a new Model with the given `name` using the
 * `definition` function provided.
 */
Datastar.prototype.define = function define(name, definition, options) {

  if (!definition && typeof name === 'function') {
    options = definition;
    definition = name;
    name = definition.name;
  } else if (!options && typeof definition === 'object') {
    options = definition;
    definition = function () {
    };
  }

  if (!name) {
    throw new Error('A name for the model is required.');
  }
  if (!definition && !options) {
    throw new Error('A definition function or options are required.');
  }

  //
  // Adapted from resourceful
  // https://github.com/flatiron/resourceful/blob/master/lib/resourceful/core.js#L82-L219
  //
  // A simple factory stub where we attach anything to the instance of the Model
  // that we deem necessary
  //
  var Factory = function Factory(data) {
    this.Model = Factory;
    this.init(data);
  };

  //
  // Setup inheritance
  // "Trust me, I'm a scientist"
  // "Back off, man. I'm a scientist." - Bill Murray
  //

  Factory.__proto__ = Model;
  Factory.prototype.__proto__ = Model.prototype;

  Understudy.call(Factory);
  // NOTE: Call definition here. Beneficial if
  // there's any non-function props being set.
  definition.call(Factory);

  //
  // Attach the connection to the factory constructor
  //
  this.attach(Factory);

  options = options || this.options;
  options.schema = options.schema || {};
  options.name = options.name || name;
  //
  // Initialize the model and the various attributes that belong there
  //
  Factory.init(options);

  return Factory;
};

Datastar.Priam = Priam;
Datastar.Understudy = Understudy;
Datastar.Model = Model;
Datastar.AwaitWrap = AwaitWrap;
