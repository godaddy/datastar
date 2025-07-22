//
// Simple pick function to select specified properties from an object
// Replacement for lodash.pick to avoid dependency vulnerabilities
//
module.exports = function pick(obj, keys) {
  const result = {};
  if (obj != null) {
    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
       result[key] = obj[key];
      }
    }
  }
  return result;
};