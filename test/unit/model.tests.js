'use strict';

var assume  = require('assume'),
    uuid    = require('uuid'),
    helpers = require('../helpers'),
    mocks   = require('../mocks'),
    schemas = require('../fixtures/schemas'),
    sinon   = require('sinon');

assume.use(require('assume-sinon'));

describe('Model (unit)', function () {
  var datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
  var id = '0c664ebb-56b4-4bf4-9e2b-509c1b5cc596';
  var Artist;

  it('should create a model even with a missing name', function () {
    var Store = datastar.define('store', {
      schema: datastar.schema.object({
        id: datastar.schema.cql.text()
      }).partitionKey('id')
    });
    assume(Store.schema.name).to.equal('store');
  });

  it('should create a model', function () {
    Artist = datastar.define('artist', {
      schema: schemas.artist
    });

    assume(Artist.schema).is.not.an('undefined');
    assume(Artist.connection).is.not.an('undefined');
  });

  it('should create', function (done) {
    // INSERT INTO [schema.name] ([allFields[0], allFields[1]]) VALUES (?, ?, ...)
    var options = {
      entity: {
        id: id
      }
    };

    Artist.create(options, function (err) {
      assume(err).to.be.an('undefined');
      done();
    });
  });

  it('should error on create when no options are passed', function (done) {
    var options = {};

    Artist.create(options, function (err) {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  //
  // TODO: This should be a new model that is created via datastar.define when
  // this is constructor based
  //
  it('should be able to define a model with ensureTables option', function (done) {
    var Oartist = datastar.define('artist', {
      schema: schemas.artist,
      ensureTables: true
    });

    Oartist.on('ensure-tables:finish', done.bind(null, null));
    Oartist.on('error', done);
  });

  it('init() function should not call ensureTables if the ensureTables option is false', function (done) {
    var subject = helpers.stubModel(sinon);
    var options = {
      name: 'artist',
      ensureTables: false,
      schema: schemas.artist
    };

    subject.init(options);
    assume(subject.ensureTables).to.not.be.called();

    done();
  });

  it('init() function should call ensureTables if the ensureTables option is true', function (done) {
    var subject = helpers.stubModel(sinon);
    var options = {
      name: 'artist',
      ensureTables: true,
      schema: schemas.artist
    };

    subject.init(options);
    assume(subject.ensureTables).to.be.called();

    done();
  });

  //
  // TODO: We need to have the mock priam error properly for some of this testing
  //
  it.skip('should be able to define a model with ensureTables option and error');

  it('On find it should emit an error if passed bad fields and no callback', function (done) {
    var stream = Artist.find();
    stream.on('error', function (err) {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should callback with an error if passed bad fields and a callback', function (done) {
    Artist.find(null, function (err) {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should error with an improper find type', function (done) {
    Artist.find({ type: 'what' }, function (err) {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should find', function (done) {
    // SELECT [fields] FROM [table] WHERE [conditions.query[0]] AND [conditionals.query[1]] FROM [schema.name]
    //
    // We assume 'all' if no type is passed
    //
    var options = {
      fields: ['name'],
      conditions: {
        artistId: uuid.v4()
      }
    };

    //
    // Remark: Because of how priam is mocked, we cannot return a proper array here but
    // this will be tested in integration
    //
    Artist.find(options, function (err) {
      assume(err).is.falsey();
      done();
    });
  });

  it('should find and return a stream if no callback is passed', function (done) {
    var options = {
      fields: ['name'],
      conditions: {
        artistId: uuid.v4()
      }
    };
    var stream = Artist.find(options);

    stream.on('readable', function () {
      var data;
      /*eslint no-cond-assign: 0*/
      /*eslint no-invalid-this: 0*/

      while ((data = this.read()) !== null) {
        assume(data).is.an('object');
      }
    });
    stream.on('end', done);


  });

  it('should run a count query', function (done) {
    var options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.count(options, function (err) {
      assume(err).is.falsey();
      done();
    });
  });

  it('should run a findFirst query', function (done) {
    var options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.findFirst(options, function (err) {
      assume(err).is.falsey();
      done();
    });

  });

  it('should run a findOne query', function (done) {
    var options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.findOne(options, function (err) {
      assume(err).to.not.exist;
      done();
    });

  });

  it('should remove a single entity', function (done) {
    var entity = { artistId: uuid.v4(), createDate: new Date() };
    Artist.remove(entity, function (err) {
      assume(err).to.be.an('undefined');
      done();
    });
  });
});
