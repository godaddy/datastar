const
  snakeCase = require('./snake-case'),
  camelCase = require('./camel-case'),
  clone     = require('clone');

//
// A class for managing state change and alias manipulation
// of modeled data.
//
class Attributes {
  constructor(instance, data) {
    this.Model = instance.Model;
    this.instance = instance;
    this.schema = this.Model.schema;
    this.data = this.schema.prepareForUse(data);
    this._isDirty = false;
    this._was = {};
    this._changed = {};
  }

  //
  // An explicit getter method for the properties in `data`
  // that manages aliases
  //
  get(name) {
    const key = this.schema.entityKeyToColumnName(name);
    const value = this.data[key];
    if (this.schema.requiresNullConversion(key)) {
      return this.schema.valueToNull(value);
    }
    if (this.schema.isKey(key)) { return value; }
    return this.schema.nullToValue(this.schema.fieldMeta(key), value);
  }

  //
  // An explicit setter method for the properties in `data`
  // that manages aliases and records a change state as well
  // as emitting a state change event if the Model has enabled it.
  //
  set(name, value) {
    if (this.schema._aliases[name]) name = this.schema._aliases[name];
    const camelName = camelCase(name);
    const snakeName = snakeCase(name);
    this._isDirty = true;
    const oldData = this.data[snakeName];

    // only track the original value in case of multiple changes
    this._was[snakeName] = this._was[snakeName] || oldData;
    this._changed[snakeName] = this.data[snakeName] = value;
    if (this.Model.options.notifyAttributeChanges) {
      this.Model.emit('attribute:change', this.instance, camelName, value, oldData);
    }
  }

  was(name) {
    return this._was[snakeCase(name)];
  }

  //
  // Return the previous value generated based on current and previous data
  //
  previous() {
    return Object.keys(this.data).reduce((prev, key) => {
      if (!(key in prev)) {
        prev[key] = this.data[key];
      }

      return prev;
    }, clone(this._was));
  }

  needsValidation() {
    let names = this.schema.keys(), data = this.data;

    if (this.schema.lookups) {
      names = names.concat(Object.keys(this.schema.lookupTables));
    }

    return names.reduce(function (memo, key) {
      // don't include keys that are undefined
      if (key in memo || key in data) {
        memo[key] = key in memo ? memo[key] : data[key];
      }
      return memo;
    }, clone(this._changed));
  }

  //
  // Public getter for state change management
  //
  isDirty(name) {
    return name ? snakeCase(name) in this._changed : this._isDirty;
  }

  toJSON(snake) {
    const data = this.schema.reNull(this.data);
    return snake ? data : this.schema.toCamelCase(data);
  }
}

module.exports = Attributes;
