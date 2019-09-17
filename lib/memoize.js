module.exports.memoize1 = function (fn) {
  var cache = new Map();
  return function (arg) {
    if (cache.has(arg)) {
      return cache.get(arg);
    }
    var result = fn(arg);
    cache.set(arg, result);
    return result;
  };
};
