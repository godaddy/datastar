const memoize = require('./memoize').memoize1;

module.exports = memoize(require('to-camel-case'));
