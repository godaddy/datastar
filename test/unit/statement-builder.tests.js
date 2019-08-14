
const fs             = require('fs'),
  path               = require('path'),
  clone              = require('clone'),
  uuid               = require('uuid'),
  assert             = require('assert'),
  assume             = require('assume'),
  schemas            = require('../fixtures/schemas'),
  StatementBuilder   = require('../../lib/statement-builder'),
  statements         = require('../../lib/statement-builder/statements'),
  Schema             = require('../../lib/schema');

const fixturesDir = path.join(__dirname, '..', 'fixtures');

const artistEntity = require(path.join(fixturesDir, 'artist-entity'));
//
// Create a statement builder.is.the factor for the various statements in
// order to correctly parse the arguments as they should.
//
describe('StatementBuilder', () => {
  const schema = new Schema('artist', schemas.artist);
  const builder = new StatementBuilder(schema);
  const fieldList = schema.fieldString();

  describe('FindStatement', () => {
    it('should return an find ALL statement if given an empty object or no options', () => {
      const statement = builder.find({ type: 'find' }, {});

      assume(statement.cql).to.equal('SELECT ' + fieldList + ' FROM artist');
    });

    it('should return an find ALL statement with allow-filtering included', () => {
      const statement = builder.find({ type: 'find', allowFiltering: true }, {});

      assume(statement.cql).to.equal('SELECT ' + fieldList + ' FROM artist ALLOW FILTERING');
    });

    it('should return an find statement with a field list if fields in options', () => {
      const statement = builder.find({
        type: 'find',
        fields: ['artist_id', 'name']
      }, {});
      assume(statement.cql).to.equal('SELECT "artist_id", "name" FROM artist');
    });

    it('should return a find statement with a limit if specified in options', () => {
      const statement = builder.find({
        type: 'all',
        limit: 2
      }, {});

      assume(statement.cql).to.equal('SELECT ' + fieldList + ' FROM artist LIMIT 2');
    });

    it('should return an error when passed conditions that get filtered (non primary keys)', () => {
      const statement = builder.find({
        type: 'find',
        conditions: {
          createDate: new Date()
        }
      });

      assume(statement).is.an.Error;
    });

    it('should return a find statement with query params', () => {
      const statement = builder.find({
        type: 'find',
        conditions: {
          artistId: { lte: '2345', gt: '1234' }
        }
      });

      assume(statement.cql).to.equal('SELECT ' + fieldList + ' FROM artist WHERE artist_id <= ? AND artist_id > ?');
      assume(statement.params.length).to.equal(2);
      assume(statement.params[0].value).to.equal('2345');
      assume(statement.params[1].value).to.equal('1234');
    });
  });

  describe('Lookup Tables', () => {
    const s = new Schema('artist', schemas.artist);
    const b = new StatementBuilder(s);
    s.setLookupKeys(['name']);
    let entity;

    beforeEach(() => {
      entity = clone(artistEntity);
    });

    it('(Find test) should return an error when passed conditions with conflicting lookup tables', () => {
      const statement = b.find({
        type: 'single',
        conditions: {
          artistId: uuid.v4(),
          name: 'whatever'
        }
      });

      assume(statement).is.instanceof(Error);
    });

    it('should return a valid statement that uses the proper lookup table for find when querying by domainName', () => {
      const statement = b.find({
        type: 'single',
        conditions: {
          name: 'whatever'
        }
      });
      //
      // Ensure we are querying the correct lookup table
      //
      assume(statement.cql.indexOf('artist_by_name'));
      assume(statement.params[0].value).to.equal('whatever');
    }
    );

    it('should return multiple statements to create for each lookup table', () => {
      const statement = b.create({}, entity);
      assume(statement.statements.length).to.equal(2);
      statement.statements.forEach(function (state) {
        assume(state.cql).is.a('string');
      });
    });

    it('should return a validation error when trying to create without all of the lookup tables', () => {
      const ent = clone(entity);
      delete ent.name;
      const statement = b.create({}, ent);
      assume(statement).is.instanceof(Error);
    });

    function setupPrevious(entity) {
      entity.name = 'something-else';
      return entity;
    }

    function modifySet(entity) {
      entity.relatedArtists = {
        add: entity.relatedArtists.slice(0, 2),
        remove: entity.relatedArtists.slice(3, 5)
      };
      return entity;
    }


    function setupOtherPrevious(entity) {
      entity.domainName = 'something-else';
      return entity;
    }

    describe('update', () => {
      it('should create a compound statement of compound statements with remove/create for looku tables', () => {
        const previous = setupPrevious(clone(artistEntity));
        entity = modifySet(clone(artistEntity));
        const statement = b.update({ previous: previous }, entity);
        assume(statement.statements.length).to.equal(2);
        // because of add remove on a set
        assume(statement.statements[0].statements.length).to.equal(2);
        assume(statement.statements[0].statements[0].cql.indexOf('UPDATE')).to.not.equal(-1);
        assume(statement.statements[1].statements.length).to.equal(2);
        // orion_id
        assume(statement.statements[1].statements[0].cql.indexOf('artist_by_name')).to.not.equal(-1);
        assume(statement.statements[1].statements[0].cql.indexOf('DELETE')).to.not.equal(-1);
        assume(statement.statements[1].statements[1].cql.indexOf('INSERT')).to.not.equal(-1);
      });

      it('should create a compound statement for lookup tables with 1 replacement for domain_name', () => {
        const previous = setupOtherPrevious(clone(artistEntity));
        entity = modifySet(clone(artistEntity));
        const statement = b.update({ previous: previous }, entity);
        assume(statement.statements.length).to.equal(2);
        // because of add remove on a set
        assume(statement.statements[0].statements.length).to.equal(2);
        assume(statement.statements[0].statements[0].cql.indexOf('UPDATE')).to.not.equal(-1);
        assume(statement.statements[1].statements.length).to.equal(2);
        // orion_id
        assume(statement.statements[1].statements[0].cql.indexOf('artist_by_name')).to.not.equal(-1);
        assume(statement.statements[1].statements[0].cql.indexOf('UPDATE')).to.not.equal(-1);
        assume(statement.statements[1].statements[1].cql.indexOf('UPDATE')).to.not.equal(-1);
      });

    });

    it('should return multiple statements to delete from each lookup table', () => {
      const statement = b.remove({}, entity);
      assume(statement.statements.length).to.equal(2);
      statement.statements.forEach(function (state) {
        assume(state.cql).is.a('string');
      });
    });

    it('should return an error when trying to delete with no conditions when there is a lookup table', () => {
      const statement = b.remove({}, {});
      assume(statement).is.instanceof(Error);
    });

    it('should return an error FOR NOW when trying to delete with insufficient conditions', () => {
      const statement = b.remove({}, { id: uuid.v4() });
      assume(statement).is.instanceof(Error);
    });
  });

  //
  // This is also tested in cases within TableStatement
  //
  describe('AlterStatement', () => {
    it('should return an error when given a bad type', () => {
      const statement = builder.alter({ type: 'NANANANA' });

      assume(statement).is.instanceof(Error);
    });

    it('should return an error when given an unknown arg type', () => {
      const statement = builder.alter({
        type: 'table',
        actions: {
          something: new RegExp()
        }
      });

      assume(statement).is.instanceof(Error);
    });
  });

  describe('TableStatment { schema } valid', () => {
    let entity;
    it('build()', done => {
      var schema = new Schema('artist', schemas.artist);
      var builder = new StatementBuilder(schema);
      const statement = builder.table({ type: 'ensure' });
      fs.readFile(path.join(fixturesDir, 'tables', 'artist.cql'), 'utf8', (err, data) => {
        assert(!err);
        assume(statement.cql.trim()).to.equal(data.trim());
        done();
      });
    });

    function compileTable(options, entity) {
      /* eslint-disable-next-line */
      const statement = new statements.table(schema);
      const opts = statement.init(options, entity);
      let tableName;
      assume(opts).to.not.be.instanceof(Error);

      if (options.lookupKey) {
        assume(options.lookupColumn.type).to.not.equal('map');
        assume(options.lookupColumn.type).to.not.equal('set');
        if (options.useIndex) {
          tableName = schema.name + '_' + options.lookupKey;
        } else {
          tableName = schema.name + '_by_' + options.lookupKey.replace(/_\w+$/, '');
        }
      }

      return statement._compile(opts, tableName, options.lookupKey);
    }


    it('should return a proper TableStatement with orderBy option', () => {
      const statement = builder.table({
        type: 'ensure',
        orderBy: { key: 'createDate', order: 'DESC' }
      });

      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('WITH CLUSTERING ORDER BY (create_date DESC);')).is.above(0);
    });

    it('should return a proper TableStatement with orderBy option without order', () => {
      const statement = builder.table({
        type: 'ensure',
        orderBy: { key: 'createDate' }
      });
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('WITH CLUSTERING ORDER BY (create_date);')).is.above(0);
    });

    it('should return an error when given a bad key to orderBy', () => {
      const statement = builder.table({
        type: 'ensure',
        orderBy: { key: 'createdAt' }
      });

      assume(statement).is.instanceof(Error);
    });

    it('should return a table statement with proper alterations', () => {
      const statement = builder.table({
        type: 'ensure',
        with: {
          compaction: {
            class: 'LeveledCompactionStrategy',
            enabled: true,
            sstableSizeInMb: 160,
            tombstoneCompactionInterval: '86400'
          },
          gcGraceSeconds: 86400,
          someOtherThing: 'LoLoLoL'
        }
      });

      assume(statement.cql.indexOf('WITH compaction = ')).is.above(0);
    });

    it('should return a proper table statement with a schema that uses composite partition keys', () => {
      const s = new Schema('cat', schemas.cat);
      const b = new StatementBuilder(s);
      const statement = b.table({ type: 'ensure' });

      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).contains('PRIMARY KEY ((cat_id, hash))');
    });

    it('should return a proper table statement with a schema that uses a secondary or clustering key', () => {
      const s = new Schema('album', schemas.album);
      const b = new StatementBuilder(s);
      const statement = b.table({ type: 'ensure' });

      assume(statement).is.not.instanceof(Error);
      assume(statement.cql).contains('PRIMARY KEY (artist_id, album_id)');
    });

    it('should return a proper TableStatement from _compile()', () => {
      entity = {};
      const options = {
        type: 'ensure',
        useIndex: false,
        lookupKey: null,
        lookupColumn: null
      };
      const cql = compileTable(options, entity);
      assume(cql).is.a('string');
      assume(cql.indexOf('CREATE TABLE IF NOT EXISTS')).is.above(-1);
      // NOTE: Not a lookup table:
      assume(cql.indexOf('PRIMARY KEY (artist_id)')).is.above(-1);
    });

    it('should return a proper index TableStatement from _compile()', () => {
      entity = {};
      const options = {
        type: 'ensure',
        useIndex: true,
        lookupKey: null,
        lookupColumn: null
      };
      const cql = compileTable(options, entity);
      assume(cql).is.a('string');
      assume(cql.indexOf('CREATE INDEX IF NOT EXISTS')).is.above(-1);
      // NOTE: Not a lookup table:
      assume(cql.indexOf('on ' + schema.name + '(artist_id)')).is.above(-1);
    });

    it('should return a proper TableStatement from build()', () => {
      const statement = builder.table({
        type: 'ensure',
        useIndex: false,
        lookupKey: null,
        lookupColumn: null
      });

      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('CREATE TABLE IF NOT EXISTS')).is.above(-1);
      // NOTE: Not a lookup table:
      assume(statement.cql.indexOf('PRIMARY KEY (artist_id)')).is.above(-1);
    });

    it('should return a proper index TableStatement from build()', () => {
      const statement = builder.table({
        type: 'ensure',
        useIndex: true,
        lookupKey: null,
        lookupColumn: null
      });

      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('CREATE INDEX IF NOT EXISTS')).is.above(-1);
      // NOTE: Not a lookup table:
      assume(statement.cql.indexOf('on ' + schema.name + '(artist_id)')).is.above(-1);
    });

    it('should return a proper TableStatement (lookup) from _compile()', () => {
      entity = {};
      const options = {
        type: 'ensure',
        useIndex: false,
        lookupKey: 'name',
        lookupColumn: { type: 'text' }
      };
      const cql = compileTable(options, entity);
      assume(cql).is.a('string');
      assume(cql.indexOf('CREATE TABLE IF NOT EXISTS')).is.above(-1);
      assume(cql.indexOf('PRIMARY KEY (' + options.lookupKey + ')')).is.above(-1);
    });

    it('should return a proper index TableStatement (lookup) from _compile()', () => {
      entity = {};
      const options = {
        type: 'ensure',
        useIndex: true,
        lookupKey: 'name',
        lookupColumn: { type: 'text' }
      };
      const cql = compileTable(options, entity);
      assume(cql).is.a('string');
      assume(cql.indexOf('CREATE INDEX IF NOT EXISTS')).is.above(-1);
      assume(cql.indexOf('on ' + schema.name + '(' + options.lookupKey + ')')).is.above(-1);
    });

    it('should return a proper TableStatement (lookup) from build()', () => {
      const options = {
        type: 'ensure',
        useIndex: false,
        lookupKey: 'name',
        lookupColumn: { type: 'text' }
      };

      const statement = builder.table(options);
      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('CREATE TABLE IF NOT EXISTS')).is.above(-1);
      assume(statement.cql.indexOf('PRIMARY KEY (' + options.lookupKey + ')')).is.above(-1);
    });

    it('should return a proper TableStatement for dropping the table from build()', () => {
      const options = {
        type: 'drop'
      };

      const statement = builder.table(options);
      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('DROP TABLE')).is.above(-1);
    });

    it('should return a proper TableStatement for dropping an index from build', () => {
      const options = {
        type: 'drop',
        useIndex: true
      };

      const statement = builder.table(options);
      assume(statement).to.not.be.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('DROP INDEX')).is.above(-1);
    });

    it('should return a proper index TableStatement (lookup) from build()', () => {
      const options = {
        type: 'ensure',
        useIndex: true,
        lookupKey: 'name',
        lookupColumn: { type: 'text' }
      };
      const statement = builder.table(options);
      assume(statement).is.not.instanceof(Error);
      assume(statement.cql).is.a('string');
      assume(statement.cql.indexOf('CREATE INDEX IF NOT EXISTS')).is.above(-1);
      assume(statement.cql.indexOf('on ' + schema.name + '(' + options.lookupKey + ')')).is.above(-1);
    });
  });

  describe('RemoveStatement', () => {
    it('should return a proper RemoveStatement passed a single entity', () => {
      const id = uuid.v4();
      //
      // Pass in an entity to remove to build the statement
      //
      const statement = builder.remove({}, { id: id, createDate: new Date() });

      assume(statement.cql).to.equal('DELETE FROM artist WHERE artist_id = ?');
      assume(statement.params[0].value).to.equal(id);
    });

    it('should return an error when passed an entity without a primary key', () => {
      const statement = builder.remove({}, { createDate: new Date() });

      assume(statement).is.an.Error;
    });

    it('should return an error when passed empty conditions ', () => {
      const statement = builder.remove({}, {});
      assume(statement).is.an.Error;
    });

    it('should return a proper RemoveStatement when passed a set of conditions', () => {
      const id = uuid.v4();
      const statement = builder.remove({
        conditions: {
          artistId: id
        }
      });

      assume(statement.cql).to.equal('DELETE FROM artist WHERE artist_id = ?');
      assume(statement.params[0].value).to.equal(id);
    });
  });

  describe('UpdateStatement', () => {
    const entity = clone(artistEntity);
    //
    // More complex case
    //
    it('should return a proper update statement when given a artist object', () => {
      const statement = builder.update({}, entity);
      assume(statement.statements.length).to.equal(1);
      assume(statement.statements[0].params.length).to.equal(6);

    });

    it('should return a proper statement with USING TTL if ttl options are passed', () => {
      const statement = builder.update({ ttl: 8643462 }, entity);

      assume(statement.statements.length).to.equal(1);
      assume(statement.statements[0].cql).contains('USING TTL 8643462');
    });

    it('should properly generate multiple statements when updating a set with add and remove', () => {
      const next = clone(entity);
      next.relatedArtists = {
        add: next.relatedArtists.slice(0, 2),
        remove: next.relatedArtists.slice(3, 5)
      };
      const statement = builder.update({}, next);

      assume(statement.statements.length).to.equal(2);

    });
  });

  describe('CreateStatement', () => {
    const entity = clone(artistEntity);
    it('should return a proper create statement when given a artist object', () => {

      const statement = builder.create({}, entity);
      //
      // TODO: better assumes for the specific entity we are dealing with to
      // ensure we are accurate in how we convert things
      //
      assume(statement.table).eqls('artist');
      assume(statement.cql).is.a('string');
      assume(statement.params).is.an('array');
    });

    it('should append USING TTL to the statement if it is passed as an option', () => {
      const statement = builder.create({ ttl: 864342 }, entity);
      assume(statement.cql).contains('USING TTL 864342');
    });

    it('should return an error when given an improper entity', () => {
      const ent = clone(entity);
      delete ent.artistId;
      const statement = builder.create({}, ent);
      assume(statement).is.instanceof(Error);
    });
  });
});
