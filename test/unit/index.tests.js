'use strict';

var assume = require('assume'),
    mocks  = require('../mocks');

describe('Datastar (unit)', function () {
  var Datastar;

  beforeEach(function () {
    Datastar = mocks.datastar();
  });

  it('should create datastar instance', function () {
    var datastar = new Datastar();
    datastar.connect(function (err) {
      assume(err).is.an('undefined');
    });
  });
});
