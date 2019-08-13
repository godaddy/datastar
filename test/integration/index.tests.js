
const assume  = require('assume'),
  Datastar   = require('../../lib'),
  helpers    = require('../helpers');

describe('Datastar', () => {
  let config;

  beforeEach(done => {
    //
    // Load our config for the current environment once.
    //
    if (config) {
      return done();
    }
    helpers.load((err, data) => {
      assume(err).equals(null);
      config = data;
      done();
    });
  });

  it('should create datastar instance without pre-heating connection', done => {
    const datastar = new Datastar({ config: config.cassandra });
    datastar.connect();
    assume(datastar.connection).is.not.an('undefined');
    datastar.close(done);
  });

  it('should create datastar instance with pre-heating connection', done => {
    const datastar = new Datastar({ config: config.cassandra });
    datastar.connect(err => {
      assume(err).is.falsey();
      assume(datastar.connection).is.not.an('undefined');
      datastar.close(done);
    });
  });
});
