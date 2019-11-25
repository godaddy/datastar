/* jshint camelcase: false */

const assume = require('assume'),
  uuid       = require('uuid'),
  async      = require('async'),
  clone      = require('clone'),
  schemas    = require('../fixtures/schemas'),
  Datastar   = require('../..'),
  helpers    = require('../helpers');

/* eslint no-invalid-this: 1*/
describe('Model', function () {
  this.timeout(60000);
  let datastar, Artist;

  before(done => {
    helpers.load((err, data) => {
      assume(err).to.equal(null);
      datastar = helpers.connectDatastar({ config: data.cassandra }, Datastar, done);
    });
  });

  describe('Artist', () => {
    after(done => {
      if (Artist) return Artist.dropTables(done);
      done();
    });

    it('should create a model', () => {
      Artist = datastar.define('artist', {
        schema: schemas.artist
      });

      Artist.before('create:build', (options, next) => {
        assume(options.statements).to.be.instanceof(datastar.StatementCollection);
        next();
      });

      assume(Artist.schema).to.not.be.an('undefined');
      assume(Artist.connection).to.not.be.an('undefined');
    });

    it('should create tables', done => {
      Artist.ensureTables(done);
    });

    it('should create', done => {
      // INSERT INTO [schema.name] ([allFields[0], allFields[1]]) VALUES (?, ?, ...)
      const options = {
        entity: {
          artistId: '00000000-0000-0000-0000-000000000002',
          name: 'hello there'
        }
      };

      Artist.create(options, done);
    });

    it('should update', done => {
      const entity = {
        id: '00000000-0000-0000-0000-000000000003',
        name: 'nirvana',
        createDate: new Date(),
        metadata: {
          randomKey: 'hello'
        }
      };

      const update = {
        id: entity.id,
        members: ['Kurt Cobain'],
        metadata: {
          hello: 'world',
          please: 'helpMe',
          what: 'can i do'
        },
        relatedArtists: [uuid.v4(), uuid.v4()]
      };


      //
      // First run a create, then do an update
      //
      /* eslint max-nested-callbacks: 1*/
      Artist.create(entity, err => {
        assume(err).is.falsey();

        //
        // Now run a find and confirm we are where we assume
        //
        Artist.get(entity.id, (_, result) => {
          assume(result.id).to.equal(entity.id);
          assume(result.name).to.equal(entity.name);
          assume(result.metadata).to.be.an('object');
          assume(result.metadata.randomKey).to.equal(entity.metadata.randomKey);

          //
          // Now update to this same entity
          //
          Artist.update(update, err => {
            assume(err).is.falsey();
            find(Artist, entity.id, (_, result) => {
              assume(result.members).to.deep.equal(update.members);
              assume(result.metadata.hello).to.equal(update.metadata.hello);
              assume(result.metadata.please).to.equal(update.metadata.please);
              assume(result.metadata.what).to.equal(update.metadata.what);
              assume(result.relatedArtists.sort()).to.deep.equal(update.relatedArtists.sort());

              //
              // By passing a null we set the map value to null
              //
              Artist.update({
                id: entity.id,
                metadata: {
                  hello: null
                }
              }, err => {
                assume(err).is.falsey();
                find(Artist, entity.id, (_, result) => {
                  assume(result.metadata.hello).is.falsey();

                  Artist.remove(entity, done);
                });
              });
            });
          });
        });
      });
    });

    it('should find', done => {
      // SELECT [fields] FROM [table] WHERE [conditions.query[0]] AND [conditionals.query[1]] FROM [schema.name]
      const options = {
        type: 'all',
        conditions: {
          artistId: '00000000-0000-0000-0000-000000000002'
        }
      };

      Artist.find(options, (err, results) => {
        assume(err).to.equal(null);
        assume(results).to.be.an('array');
        assume(results.length).equals(1);

        const result = results[0];
        assume(result.id).to.be.a('string');
        assume(result.name).to.be.a('string');
        done();
      });
    });

    //
    // Do a sequence of create, find, remove, find to validate remove worked
    // properly. We may want to put delays in here depending on how cassandra
    // behaves
    //
    it('should remove', done => {
      const id = '00000000-0000-0000-0000-000000000001';
      const entity = {
        id: id,
        name: 'meatloaf'
      };

      const findOptions = {
        type: 'all',
        conditions: {
          artistId: id
        }
      };

      Artist.create(entity, err => {
        if (err) {
          return done(err);
        }
        async.waterfall([
          Artist.find.bind(Artist, findOptions),
          (result, next) => {
            const res = result[0];
            assume(result.length).to.equal(1);
            assume(res.id).to.be.a('string');
            assume(res.name).to.be.a('string');
            next(null, res);
          },
          Artist.remove.bind(Artist),
          (_, next) => {
            setTimeout(() => {
              Artist.find(findOptions, next);
            }, 1000);
          }
        ], (err, last) => {
          if (err) {
            return done(err);
          }
          assume(last.length).to.equal(0);
          done();
        });
      });
    });
  });

  describe('update on List type', () => {
    let Person;

    const entity = {
      id: 'a2fc6faa-ca90-4b00-bfc7-e3a3dcb05be3',
      name: 'Geoff',
      characteristics: ['egotistical', 'opinionated', 'proud']
    };

    after(done => {
      if (Person) return Person.dropTables(done);
      done();
    });

    it('create a person model', done => {
      Person = datastar.define('person', {
        ensureTables: true,
        schema: schemas.person
      });

      Person.on('ensure-tables:finish', done.bind(null, null));
      Person.on('error', done);
    });

    it('should create a person model with a list', done => {
      Person.create(entity, done);
    });

    it('should handle an update operation that prepends a value on the list', done => {
      Person.update({
        id: entity.id,
        characteristics: {
          prepend: ['nosey']
        }
      }, err => {
        assume(err).is.falsey();
        setTimeout(() => {
          Person.findOne({
            conditions: { id: entity.id }
          }, (err, result) => {
            assume(err).is.falsey();
            assume(result.characteristics[0]).to.equal('nosey');
            done();
          });
        }, 1000);
      });
    });

    it('should handle an update for an append operation', done => {
      Person.update({
        id: entity.id,
        characteristics: {
          append: ['insecure']
        }
      }, err => {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, (err, result) => {
          assume(err).is.falsey();
          assume(result.characteristics[result.characteristics.length - 1]).to.equal('insecure');
          done();
        });
      });
    });

    it('should handle an update for a remove operation', done => {
      Person.update({
        id: entity.id,
        characteristics: {
          remove: ['egotistical']
        }
      }, err => {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, (err, result) => {
          assume(err).is.falsey();
          assume(result.characteristics.indexOf('egotistical')).to.equal(-1);
          done();
        });
      });
    });

    it('should handle an update for an index operation', done => {
      Person.update({
        id: entity.id,
        characteristics: {
          index: { 1: 'ego-driven' }
        }
      }, err => {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, (err, result) => {
          assume(err).is.falsey();
          assume(result.characteristics[1]).to.equal('ego-driven');
          done();
        });
      });
    });

    it('should handle an update that replaces the list', done => {
      const newChars = ['friendly', 'humble', 'present'];
      Person.update({
        id: entity.id,
        characteristics: newChars
      }, err => {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, (err, result) => {
          assume(err).is.falsey();
          assume(result.characteristics).to.deep.equal(newChars);
          done();
        });
      });
    });

    it('should fetch a person with the simpler find syntax (object)', done => {
      Person.findOne({
        id: entity.id
      }, (err, res) => {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });

    });

    it('should fetch a person with the simpler find syntax (string)', done => {
      Person.get(entity.id, (err, res) => {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });
    });

    it('should completely remove the entity', done => {
      Person.remove({
        id: entity.id
      }, done);
    });
  });

  describe('Composite Partition Keys', () => {
    let Cat;
    const id = 'c000a7a7-372a-482c-96be-e06050933725';
    const hash = 6;

    after(done => {
      if (Cat) return Cat.dropTables(done);
      done();
    });

    it('should create the table when defining the model with composite partition key', done => {
      Cat = datastar.define('cat', {
        ensureTables: true,
        schema: schemas.cat
      });

      Cat.on('ensure-tables:finish', done.bind(null, null));
      Cat.on('error', done);

      assume(Cat.schema).to.not.be.an('undefined');
      assume(Cat.connection).to.not.be.an('undefined');
    });

    it('should be able to create', done => {
      Cat.create({
        id: id,
        hash: hash,
        name: 'Hector'
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });

    it('should be able to update that same record', done => {
      Cat.update({
        id: id,
        hash: hash,
        createDate: new Date()
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });

    it('should error with simpler find syntax with more complicated key (string)', done => {
      Cat.get(id, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should be able to find the record', done => {
      Cat.findOne({
        conditions: {
          id: id,
          hash: hash
        }
      }, (err, res) => {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });
    });

    it('should error when updating without the hash in the composite key', done => {
      Cat.update({
        id: id,
        createDate: new Date()
      }, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should error when we pass in a key that doesnt exist when running update', done => {
      Cat.update({
        id: id,
        hash: hash,
        whatAReYOuDOing: 'hello'
      }, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should error when we pass in a key that doesnt exist when running create', done => {
      Cat.create({
        id: id,
        hash: hash,
        whatAReYOuDOing: 'hello'
      }, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should remove the record', done => {
      Cat.remove({
        id: id,
        hash: hash
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });
  });

  describe('Clustering Keys', () => {
    let Album;
    const id = '9adc5c0e-6de5-4cf2-9b96-143f82caba63';
    const artistId = 'd416d385-c57d-4db9-9e37-ca04cb9fceb9';

    after(done => {
      if (Album) return Album.dropTables(done);
      done();
    });

    it('should create a table with secondary/clustering keys', done => {
      Album = datastar.define('album', {
        ensureTables: true,
        schema: schemas.album
      });

      Album.on('ensure-tables:finish', done.bind(null, null));
      Album.on('error', done);

      //
      // Work with the literal object on every findOne because Why not?
      //
      Album.after('find:one', (result, next) => {
        next(null, result.toJSON());
      });
      //
      // AND THEN LETS CHANGE IT BACK
      //
      Album.after('find:one', (result, next) => {
        next(null, new Album(result));
      });
    });

    it('should create an album with proper IDs', done => {
      Album.create({
        id: id,
        artistId: artistId,
        name: 'hello',
        releaseDate: new Date()
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });

    it('should update an album', done => {
      Album.update({
        id: id,
        artistId: artistId,
        trackList: ['whatever whatever whatever', 'you dont know whats coming']
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });

    it('should find an album', done => {
      Album.findOne({
        conditions: {
          id: id,
          artistId: artistId
        }
      }, (err, res) => {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        assume(res).to.be.instanceof(Album);
        done();
      });
    });

    it('should error when updating without the artistId', done => {
      Album.update({
        id: id
      }, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should remove an Album', done => {
      Album.remove({
        id: id,
        artistId: artistId
      }, err => {
        assume(err).is.falsey();
        done();
      });
    });
  });

  describe('Lookup Tables', () => {
    let Song;
    const uniqueId = '9adc5c0e-6de5-4cf2-9b96-143f82caba63';
    const otherId = 'd416d385-c57d-4db9-9e37-ca04cb9fceb9';
    const id = 'a5fbcd74-12e1-4860-b625-db2c472ba1fa';
    let newOtherId = 'b7c49590-6b37-45c6-9d9b-2a82759c52a8';

    function findOneAll(ids, callback) {
      if (typeof ids === 'function') {
        callback = ids;
        ids = {};
      }
      async.parallel({
        otherId: Song.findOne.bind(Song, { conditions: { otherId: ids.otherId || otherId }}),
        uniqueId: Song.findOne.bind(Song, { conditions: { uniqueId: ids.uniqueId || uniqueId }}),
        id: Song.findOne.bind(Song, { conditions: { id: ids.id || id }})
      }, callback);
    }

    after(done => {
      if (Song) return Song.dropTables(done);
      done();
    });

    it('should be created when defining a model with lookupKeys and ensureTables is true', done => {
      Song = datastar.define('song', {
        ensureTables: true,
        schema: schemas.song.lookupKeys(['otherId', 'uniqueId'])
      });

      Song.on('ensure-tables:finish', done.bind(null, null));
      Song.on('error', done);

      assume(Song.schema).to.not.be.an('undefined');
      assume(Song.connection).to.not.be.an('undefined');
    });

    it('should be able to write all lookup tables', done => {
      Song.create({
        id: id,
        otherId: otherId,
        uniqueId: uniqueId
      }, done);
    });

    const previous = {
      id: id,
      otherId: otherId,
      uniqueId: uniqueId
    };

    const update = {
      id: id,
      otherId: otherId,
      uniqueId: uniqueId,
      length: '3:21',
      artists: [uuid.v4(), uuid.v4()]
    };

    it('should be able to update all lookup tables when not changing the primary keys', done => {
      Song.update({
        previous: previous,
        entity: update
      }, done);
    });

    it('should be able to find by all the `primaryKeys` and return the same value', done => {
      findOneAll((err, result) => {
        assume(err).is.falsey();
        assume(result.id).to.deep.equal(result.uniqueId);
        assume(result.uniqueId).to.deep.equal(result.otherId);
        assume(result.id).to.deep.equal(result.otherId);
        done();
      });
    });
    //
    // Setup second update
    //
    const up = clone(update);
    const newArtist = uuid.v4();
    up.artists.push(newArtist);
    up.otherId = newOtherId;

    it('should be able to update to all lookup tables when changing a primaryKey for a lookup table and ensure the old primaryKey reference was deleted', done => {
      Song.update({
        previous: [previous],
        entities: [up]
      }, err => {
        assume(err).is.falsey();
        //
        // slight delay
        //
        setTimeout(() => {
          Song.findOne({
            conditions: {
              otherId: update.otherId
            }
          }, (err, res) => {
            assume(err).is.falsey();
            assume(res).is.falsey();
            done();
          });
        }, 500);
      });
    });

    it('should find all by all primaryKeys, specifically the new one and have equal values', done => {
      findOneAll({
        otherId: newOtherId
      }, (err, result) => {
        assume(err).is.falsey();
        const id = result.id.toJSON();
        const uniqueId = result.uniqueId.toJSON();
        const otherId = result.otherId.toJSON();
        assume(id).to.deep.equal(uniqueId);
        assume(id).to.deep.equal(otherId);
        assume(uniqueId).to.deep.equal(otherId);
        done();
      });
    });

    it('should properly update when not passing a previous value and doing a find operation to fetch previous state', done => {
      Song.update({
        id: id,
        artists: {
          add: [uuid.v4()]
        }
      }, done);
    });

    it('should fail to update when calling save on the model if no columns changed directly', done => {
      Song.findOne({ id: id }, (err, song) => {
        assume(err).is.falsey();

        song.artists.push(uuid.v4());

        song.save(() => {
          Song.findOne({ id: id }, (err, result) => {
            assume(err).is.falsey();
            assume(result.toJSON()).not.to.deep.equal(song.toJSON());
            done();
          });
        });
      });
    });

    it('should update when calling save on the model and not call an extra findOne since we have the previous state', done => {
      const old = Song.findOne;
      let called = 0;

      Song.findOne = function () {
        called++;
        old.apply(Song, arguments);
      };

      Song.findOne({ id: id }, (err, song) => {
        assume(err).is.falsey();

        newOtherId = uuid.v4();
        song.otherId = newOtherId;

        song.save(err => {
          assume(err).is.falsey();
          Song.findOne({ id: id }, (err, result) => {
            assume(err).is.falsey();
            assume(result.toJSON()).to.deep.equal(song.toJSON());
            assume(called).to.equal(2);
            Song.findOne = old;
            done();
          });
        });
      });
    });

    it('should remove from all lookup tables', done => {
      Song.remove({
        id: id,
        otherId: newOtherId,
        uniqueId: uniqueId
      }, err => {
        assume(err).is.falsey();

        findOneAll({ otherId: newOtherId }, (err, result) => {
          assume(err).is.falsey();
          Object.keys(result).forEach(key => {
            assume(result[key]).is.falsey();
          });
          done();
        });
      });
    });

    it('should error when trying to remove without required attributes', done => {
      Song.remove({
        id: id,
        otherId: otherId
      }, err => {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });
  });

  describe('foo', () => {
    const zeros = '00000000-0000-0000-0000-000000000000';
    const one = uuid.v4();
    const two = uuid.v4();
    const three = uuid.v4();
    const four = uuid.v4();
    const five = uuid.v4();
    const six = uuid.v4();
    const seven = uuid.v4();
    const eight = uuid.v4();
    const nine = uuid.v4();

    let Foo;

    after(done => {
      if (Foo) return Foo.dropTables(done);
      done();
    });

    it('should create a table with an alter statement', done => {
      Foo = datastar.define('foo', {
        schema: schemas.foo,
        with: {
          compaction: {
            class: 'LeveledCompactionStrategy'
          }
        }
      });

      Foo.ensureTables(err => {
        if (err) {
          console.error(err);
          return done(err);
        }
        done();
      });
    });

    it('should create multiple records in the database', done => {
      const next = assume.wait(2, 2, done);

      Foo.create({ fooId: one, secondaryId: one, nullableId: two }, err => {
        assume(err).is.falsey();
        next();
      });

      Foo.create({ fooId: two, secondaryId: one, nullableId: two }, err => {
        assume(err).is.falsey();
        next();
      });

    });

    it('should create a record in the database that will properly expire with given ttl', done => {
      Foo.create({ entity: {
        fooId: three,
        secondaryId: zeros,
        nullableId: zeros
      }, ttl: 1 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: three, secondaryId: zeros }, (error, res) => {
          assume(error).is.falsey();
          assume(res);

          setTimeout(() => {
            Foo.findOne({ fooId: three, secondaryId: zeros }, (er, result) => {
              assume(er).is.falsey();
              assume(result).is.falsey();
              done();
            });
          }, 1100);
        });
      });
    });

    it('should create a record in the database that will expire but still be found before it reaches given ttl', done => {
      Foo.create({ entity: { fooId: four, secondaryId: one }, ttl: 7 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: four, secondaryId: one }, (err, res) => {
          assume(err).is.falsey();
          assume(res);
          assume(res.fooId).equals(four);

          setTimeout(() => {
            Foo.findOne({ fooId: four, secondaryId: one }, (err, result) => {
              assume(err).is.falsey();
              assume(result);
              assume(result.fooId).equals(four);
              done();
            });
          }, 100);
        });
      });
    });

    it('should update a record in the database that will expire with given ttl', done => {
      Foo.update({ entity: { fooId: five, secondaryId: one, something: 'foo' }, ttl: 2 }, err => {
        assume(err).is.falsey();

        setTimeout(() => {
          Foo.findOne({ fooId: five, secondaryId: one }, (er, res) => {
            assume(er).is.falsey();
            assume(res).is.falsey();
            done();
          });
        }, 2000);
      });
    });

    it('should update a record in the database that can be found before it reaches ttl', done => {
      Foo.update({ entity: { fooId: six, secondaryId: one, something: 'foo' }, ttl: 5 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: six, secondaryId: one }, (error, result) => {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(six);

          setTimeout(() => {
            Foo.findOne({ fooId: six, secondaryId: one }, (er, res) => {
              assume(er).is.falsey();
              assume(res);
              assume(res.fooId).equals(six);
              done();
            });
          }, 2000);
        });
      });
    });

    it('should update a record in the database with an updated reset ttl and can be found before it reaches the updated ttl', done => {
      Foo.update({ entity: { fooId: seven, secondaryId: one, something: 'boo' }, ttl: 1 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: seven, secondaryId: one }, (error, result) => {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(seven);

          Foo.update({ entity: { fooId: seven, secondaryId: one, something: 'foo' }, ttl: 10 }, error => {
            assume(error).is.falsey();

            setTimeout(() => {
              Foo.findOne({ fooId: seven, secondaryId: one }, (er, res) => {
                assume(er).is.falsey();
                assume(res);
                assume(res.fooId).equals(seven);
                done();
              });
            }, 1100);
          });
        });
      });
    });

    it('should update a record in the database with an updated reset ttl and expire after it reaches the updated ttl', done => {
      Foo.update({ entity: { fooId: eight, secondaryId: one, something: 'boo' }, ttl: 1 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: eight, secondaryId: one }, (error, result) => {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(eight);

          Foo.update({ entity: { fooId: eight, secondaryId: one, something: 'foo' }, ttl: 1 }, error => {
            assume(error).is.falsey();

            setTimeout(() => {
              Foo.findOne({ fooId: eight, secondaryId: one }, (er, res) => {
                assume(er).is.falsey();
                assume(res).is.falsey();
                done();
              });
            }, 1100);
          });
        });
      });
    });

    it('handles nullable fields properly', done => {
      Foo.create({ entity: {
        fooId: nine,
        secondaryId: zeros,
        nullableId: zeros
      }, ttl: 1 }, err => {
        assume(err).is.falsey();

        Foo.findOne({ fooId: nine, secondaryId: zeros }, (error, res) => {
          assume(error).is.falsey();
          assume(res);
          assume(res.fooId).equals(nine);
          assume(res.secondaryId).equals(zeros);
          assume(res.nonNullableId).equals(zeros);
          assume(res.nullableId).equals(null);

          const resAsJson = res.toJSON();
          assume(resAsJson.fooId).equals(nine);
          assume(resAsJson.secondaryId).equals(zeros);
          assume(resAsJson.nonNullableId).equals(zeros);
          assume(resAsJson.nullableId).equals(null);

          done();
        });
      });
    });

    it('should run a find query with a limit of 1 and return 1 record', done => {
      Foo.findAll({ conditions: {}, limit: 1 }, (err, recs) => {
        assume(err).is.falsey();
        assume(recs.length).equals(1);
        done();
      });
    });

    it('should remove entities', done => {
      const next = assume.wait(2, 2, done);
      Foo.remove({ fooId: one, secondaryId: one }, err => {
        assume(err).is.falsey();
        next();
      });

      Foo.remove({ fooId: two, secondaryId: one }, err => {
        assume(err).is.falsey();
        next();
      });
    });
  });

  function find(Entity, id, callback) {
    Entity.findOne({
      conditions: {
        id: id
      }
    }, (err, res) => {
      assume(err).is.falsey();
      callback(null, res);
    });
  }

  after(done => {
    datastar.close(done);
  });
});
