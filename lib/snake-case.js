var memoize = require('./memoize').memoize1;

module.exports = memoize(require('to-snake-case'));
