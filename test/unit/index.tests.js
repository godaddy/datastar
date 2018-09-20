

var assume            = require('assume'),
  datastarTestTools = require('datastar-test-tools');

var mocks = datastarTestTools.mocks;

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
