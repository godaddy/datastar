

var util = require('util'),
  With = require('../partial-statements/with'),
  Statement = require('../statement');

//
// Remark: A Statement to handle Alter operations. Its not yet known if this will
// be generic enough for all Alter statements but we will see
//
var AlterStatement = module.exports = function () {
  Statement.apply(this, arguments);

  this.types = ['TABLE'];

  this.typesLookup = this.types.reduce(function (acc, type) {
    acc[type] = true;
    return acc;
  }, {});
};

util.inherits(AlterStatement, Statement);
//
// Remark: this returns the options passed into build
//
AlterStatement.prototype._init = function (options) {
  var opts = {};
  var w;

  var actions = options.alter || options.actions || options.with || {};
  opts.type = options.type && options.type.toUpperCase();
  opts.table = options.table;
  //
  // Simple validation on type of alter statement
  //
  if (!opts.type || !this.typesLookup[opts.type]) {
    return new Error('Invalid type ' + opts.type);
  }

  //
  // Since the partial statement can error, we generate it in the init step
  // and use it later
  //
  if (actions && Object.keys(actions).length) {
    w = new With(actions);
    if (w.error) return w.error;
    opts.with = w.cql;
  }

  return opts;

};

AlterStatement.prototype.build = function (options) {
  //
  // Remark: Uppercase the type for the CQL. We might want to do some validation
  // here on type (we should actually do that in statement-builder.
  //
  var type = options.type;

  this.cql += 'ALTER ' + type + ' ';

  //
  // switch on the `type` to determine what kind of alteration we are doing.
  // This appends the specific alter command to the statement cql
  //
  switch (type) {
    case 'TABLE':
      this.xTable(options);
      break;
    default :
      break;
  }
  return this;
};

AlterStatement.prototype.xTable = function (opts) {
  var table = opts.table || this.table;

  this.cql += table + ' ';

  if (opts.with) {
    this.cql += opts.with;
  }

  return this;
};
