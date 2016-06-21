'use strict';

var assume            = require('assume'),
    datastarTestTools = require('datastar-test-tools'),
    joi               = require('joi-of-cql'),
    schemas           = require('../fixtures/schemas'),
    Schema            = require('../../lib/schema'),
    helpers           = datastarTestTools.helpers;

var debug = helpers.debug,
    cql   = joi.cql;

describe('Schema (unit)', function() {
  var schema;

  it('should create a schema', function() {
    schema = new Schema('artist', schemas.artist);
    debug(schema);

    assume(schema._columns).to.not.be.empty;
    assume(schema._primaryKeys).to.equal('artist_id');
    assume(schema.primaryKeys()).to.deep.equal(['artist_id']);
  });

  it('should throw an error when given an invalid schema', function() {
    var invalid = joi.object({
      id: cql.uuid()
    });

    function init() {
      Schema('invalid', invalid);
    }

    assume(init).throws(/must define a partitionKey/);
  });

  it('should throw an error when given an invalid name for the schema', function() {
    function init() {
      Schema('has-dashes');
    }

    assume(init).throws('Invalid character in schema name');
  });

  it('should transform the schema (snakecase and aliases)', function() {
    var entity = [
      'createDate'
    ];
    var fields = schema.fixKeys(entity);
    assume(fields).to.deep.equal(['create_date']);
    debug(fields);
  });

  it('should validate the schema', function() {
    var entity = {
      name: 'foo',
      createDate: new Date(),
      helloThere: 'new things'
    };
    debug(schema.validator);
    var valid = schema.validate(schema.fixKeys(entity));
    assume(valid.details).is.truthy();
    debug(valid);
  });

  it('should allow for null values', function() {
    var entity = {
      name: 'whocares',
      createDate: new Date(),
      relatedArtists: null
    };
    debug(schema.validator);
    var valid = schema.validate(schema.fixKeys(entity));
    assume(valid).eql(schema.fixKeys(entity));
    debug(valid);
  });
});
