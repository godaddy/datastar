
const assume = require('assume'),
  mocks      = require('../mocks');

describe('Datastar (unit)', () => {
  let Datastar;

  beforeEach(() => {
    Datastar = mocks.datastar();
  });

  it('should create datastar instance', () => {
    const datastar = new Datastar();
    datastar.connect(err => {
      assume(err).is.an('undefined');
    });
  });
});
