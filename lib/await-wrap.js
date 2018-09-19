const thenify = require('tinythen');

/**
 * The beginning experiment for async/await support for Datastar models.
 * We use this class to wrap each model and the used methods with thenables.
 * We also create a new method so we can still support streaming for findAll
 *
 * @class Wrap
 */
class Wrap {
  /**
   * Initialize instance with model
   *
   * @param {Datastar.Model} model A defined model from datastar
   * @constructor
   */
  constructor(model) {
    this.model = model;
  }
  /**
   * Thenable wrap the create method
   *
   * @function create
   * @returns {Thenable} wrapped result
   */
  create() {
    return thenify(this.model, 'create', ...arguments);
  }

  /**
   * Thenable wrap the update method
   *
   * @function update
   * @returns {Thenable} wrapped result
   */
  update() {
    return thenify(this.model, 'update', ...arguments);
  }

  /**
   * Thenable wrap the remove method
   *
   * @function remove
   * @returns {Thenable} wrapped result
   */
  remove() {
    return thenify(this.model, 'remove', ...arguments);
  }

  /**
   * Thenable wrap the findOne method
   *
   * @function findOne
   * @returns {Thenable} wrapped result
   */
  findOne() {
    return thenify(this.model, 'findOne', ...arguments);
  }
 /**
   * Thenable wrap the get method
   *
   * @function get
   * @returns {Thenable} wrapped result
   */
  get() {
    return this.findOne(...arguments);
  }
  /**
   * Return the normal model findAll for the stream
   * @function findAllStream
   * @returns {Stream} of results
   */
  findAllStream() {
    // Dont wrap this one since it can return a stream that we may want to leverage
    return this.model.findAll(...arguments);
  }

  /**
   * Thenable wrap the findAll method
   *
   * @function findAll
   * @returns {Thenable} wrapped result
   */
  findAll() {
    return thenify(this.model, 'findAll', ...arguments);
  }

  /**
   * Thenable wrap the ensureTables method
   *
   * @function ensure
   * @returns {Thenable} wrapped result
   */
  ensure() {
    return thenify(this.model, 'ensureTables');
  }
 /**
   * Thenable wrap the ensureTables method
   *
   * @function ensureTables
   * @returns {Thenable} wrapped result
   */
  ensureTables() {
    return this.ensure();
  }

  /**
   * Thenable wrap the dropTables method
   *
   * @function drop
   * @returns {Thenable} wrapped result
   */
  drop() {
    return thenify(this.model, 'dropTables');
  }
  /**
   * Thenable wrap the dropTables method
   *
   * @function drop
   * @returns {Thenable} wrapped result
   */
  dropTables() {
    return this.drop();
  }
  /**
   * Get the raw cassandra-driver because we need to do some special shit for
   * counters
   * @returns {Thenable} raw cassandra driver
   */
  _getConnection() {
    return thenify(this.model.connection, 'getConnectionPool', null, false);
  }
}

module.exports = Wrap;
