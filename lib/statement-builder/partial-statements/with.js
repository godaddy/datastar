var snakeCase = require('../../snake-case');
var util = require('util');

var specialActionMap = {
  orderBy: {
    cql: 'CLUSTERING ORDER BY (%s'
  }
};

module.exports = With;
//
// What is a partial-statement? We are going to assume its a simple string
// building function that gets passed options, returns a string or an error.
//
// We'll also make the assumption that they can be context unaware
//

function With(opts) {
  if (!(this instanceof With))
    return new With(opts);

  this.cql = 'WITH ';
  this.error = null;

  var result = this.process(opts);

  if (type(result) !== 'error') {
    this.cql += result;
  } else {
    this.error = result;
  }
}

//
// Lets assume we have a set of actions to do for
//
// An example of what we expect to receive here.
// We handle each data structure differently because we attempt to output it as
// a string representation wrapped in the proper quotes for the text.
// {
//    compaction: { /*object of compaction options*/ },
//    gcGraceSeconds: 9680
// }
//
// and the return value here
// WITH compaction = {
//  'some_setting': 'someValue'
// } AND gc_grace_seconds = 9680;
//
With.prototype.process = function process(opts) {
  var error;
  var string = Object.keys(opts)
    .map(function (action) {
      var args = opts[action];
      var executed = snakeCase(action);
      var typeArg = type(args);
      //
      // Figure out what to do based on the type of args
      // and the action
      //
      switch (typeArg) {
        case 'object':
          //
          // Special cases so we can be generic with this statement
          //
          if (specialActionMap[action])
            return this[action](args, specialActionMap[action]);
          //
          // Remark: Convert the object representation into a string
          // This is currently used for compaction as an example
          //
          return executed + ' = ' + this[typeArg](args);
        //
        // Wrap quotes around the string types
        //
        case 'string':
          return executed + " = '" + args + "'";
        case 'number':
          return executed + ' = ' + args;
        default:
          error = new Error(
            util.format('Cannot create with statement with %s %s', typeArg, args)
          );
      }
      //
      // This might not be the only separator for these types of commands so
      // this might need more variability
      //


    }, this).join(' AND ');

  return error ? error : string;
};

//
// Handle turning an object into a string for certain configuration
//
With.prototype.object = function object(mapping) {
  return '{ \n' + Object.keys(mapping).map(function (key, i) {
    //
    // Check if we have to prefix with a comma to build the json object
    //
    var sub = i !== 0 ? ' ,' : '  ';
    //
    // Translate any keys to snake_case because thats what seems
    // reasonable
    //
    return sub + util.format("  '%s' : '%s'", snakeCase(key), mapping[key]);
  }).join('\n') + ' }';
};

//
// Handle specific orderBy syntax
//
With.prototype.orderBy = function orderBy(args, opts) {
  var cql = util.format(opts.cql, args.key);
  if (args.order) cql += ' ' + args.order;
  cql += ')';
  return cql;
};

function type(of) {
  return Object.prototype.toString.call(of).slice(8, -1).toLowerCase();
}
