'use strict';

var assume = require('assume'),
    Datastar  = require('../../lib'),
    helpers = require('../helpers');


describe('Datastar', function () {
  var config;

  beforeEach(function (done) {
    //
    // Load our config for the current environment once.
    //
    if (config) {
      return done();
    }
    helpers.load(function (err, data) {
      assume(err).equals(null);
      config = data;
      done();
    });
  });

  it('should create datastar instance without pre-heating connection', function (done) {
    var datastar = new Datastar({ config: config.cassandra });
    datastar.connect();
    assume(datastar.connection).is.not.an('undefined');
    datastar.close(done);
  });

  it('should create datastar instance with pre-heating connection', function (done) {
    var datastar = new Datastar({ config: config.cassandra });
    datastar.connect(function (err) {
      assume(err).is.falsey();
      assume(datastar.connection).is.not.an('undefined');
      datastar.close(done);
    });
  });
});
