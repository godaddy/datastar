
const assume = require('assume'),
  uuid       = require('uuid'),
  schemas    = require('../fixtures/schemas'),
  sinon      = require('sinon'),
  mocks      = require('../mocks'),
  helpers    = require('../helpers');

assume.use(require('assume-sinon'));

describe('Model (unit)', () => {
  const datastar = helpers.connectDatastar({ mock: true }, mocks.datastar());
  const id = '0c664ebb-56b4-4bf4-9e2b-509c1b5cc596';
  let Artist;

  it('should create a model even with a missing name', () => {
    const Store = datastar.define('store', {
      schema: datastar.schema.object({
        id: datastar.schema.cql.text()
      }).partitionKey('id')
    });
    assume(Store.schema.name).to.equal('store');
  });

  it('should create a model', () => {
    Artist = datastar.define('artist', {
      schema: schemas.artist
    });

    assume(Artist.schema).is.not.an('undefined');
    assume(Artist.connection).is.not.an('undefined');
  });

  it('should create', done => {
    // INSERT INTO [schema.name] ([allFields[0], allFields[1]]) VALUES (?, ?, ...)
    const options = {
      entity: {
        id: id
      }
    };

    Artist.create(options, err => {
      assume(err).to.be.an('undefined');
      done();
    });
  });

  it('should error on create when no options are passed', done => {
    const options = {};

    Artist.create(options, err => {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  //
  // TODO: This should be a new model that is created via datastar.define when
  // this is constructor based
  //
  it('should be able to define a model with ensureTables option', done => {
    const Oartist = datastar.define('artist', {
      schema: schemas.artist,
      ensureTables: true
    });

    Oartist.on('ensure-tables:finish', done.bind(null, null));
    Oartist.on('error', done);
  });

  it('init() function should not call ensureTables if the ensureTables option is false', done => {
    const subject = helpers.stubModel(sinon);
    const options = {
      name: 'artist',
      ensureTables: false,
      schema: schemas.artist
    };

    subject.init(options);
    assume(subject.ensureTables).to.not.be.called();

    done();
  });

  it('init() function should call ensureTables if the ensureTables option is true', done => {
    const subject = helpers.stubModel(sinon);
    const options = {
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

  it('On find it should emit an error if passed bad fields and no callback', done => {
    const stream = Artist.find();
    stream.on('error', err => {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should callback with an error if passed bad fields and a callback', done => {
    Artist.find(null, err => {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should error with an improper find type', done => {
    Artist.find({ type: 'what' }, err => {
      assume(err).is.instanceof(Error);
      done();
    });
  });

  it('should find', done => {
    // SELECT [fields] FROM [table] WHERE [conditions.query[0]] AND [conditionals.query[1]] FROM [schema.name]
    //
    // We assume 'all' if no type is passed
    //
    const options = {
      fields: ['name'],
      conditions: {
        artistId: uuid.v4()
      }
    };

    //
    // Remark: Because of how priam is mocked, we cannot return a proper array here but
    // this will be tested in integration
    //
    Artist.find(options, err => {
      assume(err).is.falsey();
      done();
    });
  });

  it('should find and return a stream if no callback is passed', done => {
    const options = {
      fields: ['name'],
      conditions: {
        artistId: uuid.v4()
      }
    };
    const stream = Artist.find(options);

    stream.on('readable', function () {
      let data;
      /* eslint no-cond-assign: 0*/
      /* eslint no-invalid-this: 0*/

      while ((data = this.read()) !== null) {
        assume(data).is.an('object');
      }
    });
    stream.on('end', done);


  });

  it('should run a count query', done => {
    const options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.count(options, err => {
      assume(err).is.falsey();
      done();
    });
  });

  it('should run a findFirst query', done => {
    const options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.findFirst(options, err => {
      assume(err).is.falsey();
      done();
    });

  });

  it('should run a findOne query', done => {
    const options = {
      conditions: {
        artistId: uuid.v4()
      }
    };

    Artist.findOne(options, err => {
      assume(err).to.not.exist;
      done();
    });

  });

  it('should remove a single entity', done => {
    const entity = { artistId: uuid.v4(), createDate: new Date() };
    Artist.remove(entity, err => {
      assume(err).to.be.an('undefined');
      done();
    });
  });
});
