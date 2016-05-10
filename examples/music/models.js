'use strict';

var models = module.exports = function (datastar) {
  return new Models(datastar);
};

function Models(datastar) {
  this.Album = require('./album')(datastar, this);
  this.Artist = require('./artist')(datastar, this);
}
