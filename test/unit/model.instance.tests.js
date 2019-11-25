
const assume = require('assume'),
  dogFixture = require('../fixtures/dog'),
  schemas    = require('../fixtures/schemas'),
  mocks      = require('../mocks'),
  helpers    = require('../helpers'),
  cloneDeep  = require('lodash.clonedeep');

assume.use(require('assume-sinon'));

describe('Model instance (unit)', () => {
  let dog, Dog, datastar, entity;

  beforeEach(() => {
    datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
    Dog = datastar.define('dog', {
      schema: schemas.dog
    });
    entity = cloneDeep(dogFixture);
    dog = Dog.toInstance(entity);
  });

  it('should "transform" data into an instance of the defined model', () => {
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

    it('should throw the validation error', () => {
      dog.id = 'invalid guid';
      assume(() => dog.validate()).throws();
    });
  });

  it('should handle de-nulling cyclic objects', () => {
    dog.owner.puppies = [dog]; // inject cyclic reference
    // See https://github.com/godaddy/datastar/pull/27
    // Previously, this would cause recursive loop (Max call stack size exceeded error)
    assume(dog.owner).exists();
  });
});

