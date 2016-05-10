'use strict';

var snakeCase = require('to-snake-case'),
    camelCase = require('to-camel-case'),
    clone     = require('clone');

module.exports = Attributes;

//
// A class for managing state change and alias manipulation
// of modeled data.
//
function Attributes(instance, data) {
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
Attributes.prototype.get = function (name) {
  if (this.schema._aliases[name]) name = this.schema._aliases[name];
  return this.schema.valueToNull(this.data[snakeCase(name)]);
};

//
// An explicit setter method for the properties in `data`
// that manages aliases and records a change state as well
// as emitting a state change event if the Model has enabled it.
//
Attributes.prototype.set = function (name, value) {
  if (this.schema._aliases[name]) name = this.schema._aliases[name];
  var camelName = camelCase(name);
  var snakeName = snakeCase(name);
  this._isDirty = true;
  var oldData = this.data[snakeName];
  var self = this;
  // only track the original value in case of multiple changes
  this._was[snakeName] = this._was[snakeName] || oldData;
  this._changed[snakeName] = this.data[snakeName] = value;
  if (this.Model.options.notifyAttributeChanges) {
    this.Model.emit('attribute:change', self.instance, camelName, value, oldData);
  }
};

Attributes.prototype.was = function (name) {
  return this._was[snakeCase(name)];
};

//
// Return the previous value generated based on current and previous data
//
Attributes.prototype.previous = function () {
  var self = this;
  return Object.keys(this.data).reduce(function (prev, key) {
    if (!(key in prev)) {
      prev[key] = self.data[key];
    }

    return prev;
  }, clone(this._was));
};

Attributes.prototype.needsValidation = function () {
  var names = this.schema.keys(), data = this.data;

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
};

//
// Public getter for state change management
//
Attributes.prototype.isDirty = function (name) {
  return name ? snakeCase(name) in this._changed : this._isDirty;
};

Attributes.prototype.toJSON = function (snake) {
  return snake ? this.data : this.schema.toCamelCase(this.data);
};

