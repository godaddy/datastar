

var assume            = require('assume'),

  dataStarTestTools = require('datastar-test-tools'),
  entity            = require('../fixtures/dog'),
  schemas           = require('../fixtures/schemas');

var helpers = dataStarTestTools.helpers,
  mocks   = dataStarTestTools.mocks;

assume.use(require('assume-sinon'));

describe('Model instance (unit)', function () {
  var dog;
  var datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
  var Dog = datastar.define('dog', {
    schema: schemas.dog
  });

  it('should "transform" data into an instance of the defined model', function () {
    dog = Dog.toInstance(entity);
    assume(dog).is.instanceof(Dog);
  });

  describe('json type handling', function () {
    it('should deserialize a json property', function () {
      assume(dog.owner.name).is.equal('John Doe');
    });
  });

  describe('Stringify an array, toJSON handling', function () {
    it('should contain the camelCase key when an array is stringified rather than snake_case', function () {
      var ary = [dog];
      assume(JSON.stringify(ary).indexOf('dogThing')).is.not.equal(-1);
    });
  });

  describe('#validate', function () {
    it('should validate the current data against the schema validation', function () {
      dog.weight = 80;
      assume(dog.validate()).is.deep.equal({ id: dog.id, weight: 80 });
    });

    it('should return the validation error', function () {
      dog.id = 'invalid guid';
      assume(dog.validate()).is.instanceof(Error);
    });
  });
});

