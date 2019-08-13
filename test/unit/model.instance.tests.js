
const assume = require('assume'),
  entity     = require('../fixtures/dog'),
  schemas    = require('../fixtures/schemas'),
  mocks      = require('../mocks'),
  helpers    = require('../helpers');

assume.use(require('assume-sinon'));

describe('Model instance (unit)', () => {
  let dog;
  const datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
  const Dog = datastar.define('dog', {
    schema: schemas.dog
  });

  it('should "transform" data into an instance of the defined model', () => {
    dog = Dog.toInstance(entity);
    assume(dog).is.instanceof(Dog);
  });

  describe('json type handling', () => {
    it('should deserialize a json property', () => {
      assume(dog.owner.name).is.equal('John Doe');
    });
  });

  describe('Stringify an array, toJSON handling', () => {
    it('should contain the camelCase key when an array is stringified rather than snake_case', () => {
      const ary = [dog];
      assume(JSON.stringify(ary).indexOf('dogThing')).is.not.equal(-1);
    });
  });

  describe('#validate', () => {
    it('should validate the current data against the schema validation', () => {
      dog.weight = 80;
      assume(dog.validate()).is.deep.equal({ id: dog.id, weight: 80 });
    });

    it('should return the validation error', () => {
      dog.id = 'invalid guid';
      assume(dog.validate()).is.instanceof(Error);
    });
  });
});

