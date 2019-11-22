'use strict';

module.exports = function (datastar) {
  return {
    Album: require('./album')(datastar),
    Artist: require('./artist')(datastar)
  };
};
