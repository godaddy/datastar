const assume = require('assume');
const { AwaitWrap } = require('../..');
const Stream  = require('stream');

const { mocks, helpers } = require('datastar-test-tools');

describe('datastar-await-wrap', function () {
  let Model;
  let wrapped;
  let data = { name: 'what' };

  before(function () {
    const datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
    const cql = datastar.schema.cql;
    Model = datastar.define('model', {
      schema: datastar.schema.object({
        name: cql.text()
      }).partitionKey('name')
    });
    wrapped = new AwaitWrap(Model);
  });

  it('should correctly wrap the create function', async () => {
    await wrapped.create(data);
  });

  it('should correctly wrap the update function', async () => {
    await wrapped.update(data);
  });

  it('should correctly wrap the remove function', async () => {
    await wrapped.remove(data);
  });

  it('should correctly wrap the findOne function', async () => {
    await wrapped.findOne(data);
  });

  it('should correctly wrap the findAll function', async () => {
    await wrapped.findAll(data);
  });

  it('should return a stream from the findAllStream function', () => {
    const strm = wrapped.findAllStream(data);
    assume(strm).is.instanceof(Stream);
  });

  it('should wrap ensureTables as the ensure function', async () => {
    await wrapped.ensure();
  });

  it('should wrap dropTables as the drop function', async () => {
    await wrapped.drop();
  });

});
