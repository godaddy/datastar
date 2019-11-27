
const assume = require('assume'),
  joi        = require('joi-of-cql'),
  schemas    = require('../fixtures/schemas'),
  Schema     = require('../../lib/schema'),
  helpers    = require('../helpers');

const debug = helpers.debug,
  cql       = joi.cql;

describe('Schema (unit)', () => {
  let schema;

  it('should create a schema', () => {
    schema = new Schema('artist', schemas.artist);
    debug(schema);

    assume(schema._columns).to.not.be.empty;
    assume(schema._primaryKeys).to.equal('artist_id');
    assume(schema.primaryKeys()).to.deep.equal(['artist_id']);
  });

  it('should throw an error when given an invalid schema', () => {
    const invalid = joi.object({
      id: cql.uuid()
    });

    function init() {
      // eslint-disable-next-line
      new Schema('invalid', invalid);
    }

    assume(init).throws(/must define a partitionKey/);
  });

  it('should throw an error when given an invalid name for the schema', () => {
    function init() {
      // eslint-disable-next-line
      new Schema('has-dashes');
    }

    assume(init).throws('Invalid character in schema name');
  });

  it('should transform the schema (snakecase and aliases)', () => {
    const entity = [
      'createDate'
    ];
    const fields = schema.fixKeys(entity);
    assume(fields).to.deep.equal(['create_date']);
    debug(fields);
  });

  it('should validate the schema', () => {
    const entity = {
      name: 'foo',
      createDate: new Date(),
      helloThere: 'new things'
    };
    debug(schema.validator);
    assume(() => schema.validate(schema.fixKeys(entity))).throws();
  });

  it('should allow for null values', () => {
    const entity = {
      name: 'whocares',
      createDate: new Date(),
      relatedArtists: null
    };
    debug(schema.validator);
    const valid = schema.validate(schema.fixKeys(entity));
    assume(valid).eql(schema.fixKeys(entity));
    debug(valid);
  });

  it('#fieldString() should return a list of all fields suitable for CQL ' +
     'consumption if no arguments are given', () => {
    schema = new Schema('artist', schemas.artist);
    assume(schema.fieldString()).eql(
      '"artist_id", "name", "create_date", "update_date", "members", "related_artists", "traits", "metadata"');
  });

  it('#fieldString() should return a list of fields suitable for CQL ' +
     'consumption when a list of fields is given', () => {
    schema = new Schema('artist', schemas.artist);
    assume(schema.fieldString(['artist_id', 'name'])).eql('"artist_id", "name"');
  });
});
