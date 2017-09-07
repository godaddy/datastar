'use strict';

/*jshint camelcase: false */

var assume            = require('assume'),
    uuid              = require('uuid'),
    async             = require('async'),
    clone             = require('clone'),
    schemas           = require('../fixtures/schemas'),
    Datastar          = require('../..'),
    datastarTestTools = require('datastar-test-tools');

var helpers = datastarTestTools.helpers;

/*eslint no-invalid-this: 1*/
describe('Model', function () {
  this.timeout(60000);
  var datastar, Artist;

  before(function (done) {
    helpers.load(function (err, data) {
      assume(err).to.equal(null);
      datastar = helpers.connectDatastar({ config: data.cassandra }, Datastar, done);
      /* cassandra = new driver.Client({
       contactPoints: data.cassandra.hosts,
       keyspace: data.cassandra.keyspace
       });*/
    });
  });

  describe('Artist', function () {
    after(function (done) {
      if (Artist) return Artist.dropTables(done);
      done();
    });

    it('should create a model', function () {
      Artist = datastar.define('artist', {
        schema: schemas.artist
      });

      Artist.before('create:build', function (options, next) {
        assume(options.statements).to.be.instanceof(datastar.StatementCollection);
        next();
      });

      assume(Artist.schema).to.not.be.an('undefined');
      assume(Artist.connection).to.not.be.an('undefined');
    });

    it('should create tables', function (done) {
      Artist.ensureTables(done);
    });

    it('should create', function (done) {
      // INSERT INTO [schema.name] ([allFields[0], allFields[1]]) VALUES (?, ?, ...)
      var options = {
        entity: {
          artistId: '00000000-0000-0000-0000-000000000002',
          name: 'hello there'
        }
      };

      Artist.create(options, done);
    });

    it('should update', function (done) {
      var entity = {
        id: '00000000-0000-0000-0000-000000000003',
        name: 'nirvana',
        createDate: new Date(),
        metadata: {
          randomKey: 'hello'
        }
      };

      var update = {
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
      /*eslint max-nested-callbacks: 1*/
      Artist.create(entity, function (err) {
        assume(err).is.falsey();

        //
        // Now run a find and confirm we are where we assume
        //
        Artist.get(entity.id, function (_, result) {
          assume(result.id).to.equal(entity.id);
          assume(result.name).to.equal(entity.name);
          assume(result.metadata).to.be.an('object');
          assume(result.metadata.randomKey).to.equal(entity.metadata.randomKey);

          //
          // Now update to this same entity
          //
          Artist.update(update, function (err) {
            assume(err).is.falsey();
            find(Artist, entity.id, function (_, result) {
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
              }, function (err) {
                assume(err).is.falsey();
                find(Artist, entity.id, function (_, result) {
                  assume(result.metadata.hello).is.falsey();

                  Artist.remove(entity, done);
                });
              });
            });
          });
        });
      });
    });

    it('should find', function (done) {
      // SELECT [fields] FROM [table] WHERE [conditions.query[0]] AND [conditionals.query[1]] FROM [schema.name]
      var options = {
        type: 'all',
        conditions: {
          artistId: '00000000-0000-0000-0000-000000000002'
        }
      };

      Artist.find(options, function (err, results) {
        assume(err).to.equal(null);
        assume(results).to.be.an('array');
        assume(results.length).equals(1);

        var result = results[0];
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
    it('should remove', function (done) {
      var id = '00000000-0000-0000-0000-000000000001';
      var entity = {
        id: id,
        name: 'meatloaf'
      };

      var findOptions = {
        type: 'all',
        conditions: {
          artistId: id
        }
      };

      Artist.create(entity, function (err) {
        if (err) {
          return done(err);
        }
        async.waterfall([
          Artist.find.bind(Artist, findOptions),
          function (result, next) {
            var res = result[0];
            assume(result.length).to.equal(1);
            assume(res.id).to.be.a('string');
            assume(res.name).to.be.a('string');
            next(null, res);
          },
          Artist.remove.bind(Artist),
          function (_, next) {
            setTimeout(function () {
              Artist.find(findOptions, next);
            }, 1000);
          }
        ], function (err, last) {
          if (err) {
            return done(err);
          }
          assume(last.length).to.equal(0);
          done();
        });
      });
    });
  });

  describe('update on List type', function () {
    var Person;

    var entity = {
      id: 'a2fc6faa-ca90-4b00-bfc7-e3a3dcb05be3',
      name: 'Geoff',
      characteristics: ['egotistical', 'opinionated', 'proud']
    };

    after(function (done) {
      if (Person) return Person.dropTables(done);
      done();
    });

    it('create a person model', function (done) {
      Person = datastar.define('person', {
        ensureTables: true,
        schema: schemas.person
      });

      Person.on('ensure-tables:finish', done.bind(null, null));
      Person.on('error', done);
    });

    it('should create a person model with a list', function (done) {
      Person.create(entity, done);
    });

    it('should handle an update operation that prepends a value on the list', function (done) {
      Person.update({
        id: entity.id,
        characteristics: {
          prepend: ['nosey']
        }
      }, function (err) {
        assume(err).is.falsey();
        setTimeout(function () {
          Person.findOne({
            conditions: { id: entity.id }
          }, function (err, result) {
            assume(err).is.falsey();
            assume(result.characteristics[0]).to.equal('nosey');
            done();
          });
        }, 1000);
      });
    });

    it('should handle an update for an append operation', function (done) {
      Person.update({
        id: entity.id,
        characteristics: {
          append: ['insecure']
        }
      }, function (err) {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, function (err, result) {
          assume(err).is.falsey();
          assume(result.characteristics[result.characteristics.length - 1]).to.equal('insecure');
          done();
        });
      });
    });

    it('should handle an update for a remove operation', function (done) {
      Person.update({
        id: entity.id,
        characteristics: {
          remove: ['egotistical']
        }
      }, function (err) {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, function (err, result) {
          assume(err).is.falsey();
          assume(result.characteristics.indexOf('egotistical')).to.equal(-1);
          done();
        });
      });
    });

    it('should handle an update for an index operation', function (done) {
      Person.update({
        id: entity.id,
        characteristics: {
          index: { '1': 'ego-driven' }
        }
      }, function (err) {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, function (err, result) {
          assume(err).is.falsey();
          assume(result.characteristics[1]).to.equal('ego-driven');
          done();
        });
      });
    });

    it('should handle an update that replaces the list', function (done) {
      var newChars = ['friendly', 'humble', 'present'];
      Person.update({
        id: entity.id,
        characteristics: newChars
      }, function (err) {
        assume(err).is.falsey();
        Person.findOne({
          conditions: { id: entity.id }
        }, function (err, result) {
          assume(err).is.falsey();
          assume(result.characteristics).to.deep.equal(newChars);
          done();
        });
      });
    });

    it('should fetch a person with the simpler find syntax (object)', function (done) {
      Person.findOne({
        id: entity.id
      }, function (err, res) {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });

    });

    it('should fetch a person with the simpler find syntax (string)', function (done) {
      Person.get(entity.id, function (err, res) {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });
    });

    it('should completely remove the entity', function (done) {
      Person.remove({
        id: entity.id
      }, done);
    });
  });

  describe('Composite Partition Keys', function () {
    var Cat;
    var id = 'c000a7a7-372a-482c-96be-e06050933725';
    var hash = 6;

    after(function (done) {
      if (Cat) return Cat.dropTables(done);
      done();
    });

    it('should create the table when defining the model with composite partition key', function (done) {
      Cat = datastar.define('cat', {
        ensureTables: true,
        schema: schemas.cat
      });

      Cat.on('ensure-tables:finish', done.bind(null, null));
      Cat.on('error', done);

      assume(Cat.schema).to.not.be.an('undefined');
      assume(Cat.connection).to.not.be.an('undefined');
    });

    it('should be able to create', function (done) {
      Cat.create({
        id: id,
        hash: hash,
        name: 'Hector'
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });

    it('should be able to update that same record', function (done) {
      Cat.update({
        id: id,
        hash: hash,
        createDate: new Date()
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });

    it('should error with simpler find syntax with more complicated key (string)', function (done) {
      Cat.get(id, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should be able to find the record', function (done) {
      Cat.findOne({
        conditions: {
          id: id,
          hash: hash
        }
      }, function (err, res) {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        done();
      });
    });

    it('should error when updating without the hash in the composite key', function (done) {
      Cat.update({
        id: id,
        createDate: new Date()
      }, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should error when we pass in a key that doesnt exist when running update', function (done) {
      Cat.update({
        id: id,
        hash: hash,
        whatAReYOuDOing: 'hello'
      }, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should error when we pass in a key that doesnt exist when running create', function (done) {
      Cat.create({
        id: id,
        hash: hash,
        whatAReYOuDOing: 'hello'
      }, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should remove the record', function (done) {
      Cat.remove({
        id: id,
        hash: hash
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });
  });

  describe('Clustering Keys', function () {
    var Album;
    var id = '9adc5c0e-6de5-4cf2-9b96-143f82caba63';
    var artistId = 'd416d385-c57d-4db9-9e37-ca04cb9fceb9';

    after(function (done) {
      if (Album) return Album.dropTables(done);
      done();
    });

    it('should create a table with secondary/clustering keys', function (done) {
      Album = datastar.define('album', {
        ensureTables: true,
        schema: schemas.album
      });

      Album.on('ensure-tables:finish', done.bind(null, null));
      Album.on('error', done);

      //
      // Work with the literal object on every findOne because Why not?
      //
      Album.after('find:one', function (result, next) {
        next(null, result.toJSON());
      });
      //
      // AND THEN LETS CHANGE IT BACK
      //
      Album.after('find:one', function (result, next) {
        next(null, new Album(result));
      });
    });

    it('should create an album with proper IDs', function (done) {
      Album.create({
        id: id,
        artistId: artistId,
        name: 'hello',
        releaseDate: new Date()
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });

    it('should update an album', function (done) {
      Album.update({
        id: id,
        artistId: artistId,
        trackList: ['whatever whatever whatever', 'you dont know whats coming']
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });

    it('should find an album', function (done) {
      Album.findOne({
        conditions: {
          id: id,
          artistId: artistId
        }
      }, function (err, res) {
        assume(err).is.falsey();
        assume(res).to.be.an('object');
        assume(res).to.be.instanceof(Album);
        done();
      });
    });

    it('should error when updating without the artistId', function (done) {
      Album.update({
        id: id
      }, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });

    it('should remove an Album', function (done) {
      Album.remove({
        id: id,
        artistId: artistId
      }, function (err) {
        assume(err).is.falsey();
        done();
      });
    });
  });

  describe('Lookup Tables', function () {
    var Song;
    var uniqueId = '9adc5c0e-6de5-4cf2-9b96-143f82caba63';
    var otherId = 'd416d385-c57d-4db9-9e37-ca04cb9fceb9';
    var id = 'a5fbcd74-12e1-4860-b625-db2c472ba1fa';
    var newOtherId = 'b7c49590-6b37-45c6-9d9b-2a82759c52a8';

    function findOneAll(ids, callback) {
      if (typeof ids === 'function') {
        callback = ids;
        ids = {};
      }
      async.parallel({
        otherId: Song.findOne.bind(Song, { conditions: { otherId: ids.otherId || otherId } }),
        uniqueId: Song.findOne.bind(Song, { conditions: { uniqueId: ids.uniqueId || uniqueId } }),
        id: Song.findOne.bind(Song, { conditions: { id: ids.id || id } })
      }, callback);
    }

    after(function (done) {
      if (Song) return Song.dropTables(done);
      done();
    });

    it('should be created when defining a model with lookupKeys and ensureTables is true', function (done) {
      Song = datastar.define('song', {
        ensureTables: true,
        schema: schemas.song.lookupKeys(['otherId', 'uniqueId'])
      });

      Song.on('ensure-tables:finish', done.bind(null, null));
      Song.on('error', done);

      assume(Song.schema).to.not.be.an('undefined');
      assume(Song.connection).to.not.be.an('undefined');
    });

    it('should be able to write all lookup tables', function (done) {
      Song.create({
        id: id,
        otherId: otherId,
        uniqueId: uniqueId
      }, done);
    });

    var previous = {
      id: id,
      otherId: otherId,
      uniqueId: uniqueId
    };

    var update = {
      id: id,
      otherId: otherId,
      uniqueId: uniqueId,
      length: '3:21',
      artists: [uuid.v4(), uuid.v4()]
    };

    it('should be able to update all lookup tables when not changing the primary keys', function (done) {
      Song.update({
        previous: previous,
        entity: update
      }, done);
    });

    it('should be able to find by all the `primaryKeys` and return the same value', function (done) {
      findOneAll(function (err, result) {
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
    var up = clone(update);
    var newArtist = uuid.v4();
    up.artists.push(newArtist);
    up.otherId = newOtherId;

    it('should be able to update to all lookup tables when changing a primaryKey for a lookup table and ensure the old primaryKey reference was deleted', function (done) {
      Song.update({
        previous: [previous],
        entities: [up]
      }, function (err) {
        assume(err).is.falsey();
        //
        // slight delay
        //
        setTimeout(function () {
          Song.findOne({
            conditions: {
              otherId: update.otherId
            }
          }, function (err, res) {
            assume(err).is.falsey();
            assume(res).is.falsey();
            done();
          });
        }, 500);
      });
    });

    it('should find all by all primaryKeys, specifically the new one and have equal values', function (done) {
      findOneAll({
        otherId: newOtherId
      }, function (err, result) {
        assume(err).is.falsey();
        var id = result.id.toJSON();
        var uniqueId = result.uniqueId.toJSON();
        var otherId = result.otherId.toJSON();
        assume(id).to.deep.equal(uniqueId);
        assume(id).to.deep.equal(otherId);
        assume(uniqueId).to.deep.equal(otherId);
        done();
      });
    });

    it('should properly update when not passing a previous value and doing a find operation to fetch previous state', function (done) {
      Song.update({
        id: id,
        artists: {
          add: [uuid.v4()]
        }
      }, done);
    });

    it('should fail to update when calling save on the model if no columns changed directly', function (done) {
      Song.findOne({ id: id }, function (err, song) {
        assume(err).is.falsey();

        song.artists.push(uuid.v4());

        song.save(function () {
          Song.findOne({ id: id }, function (err, result) {
            assume(err).is.falsey();
            assume(result.toJSON()).not.to.deep.equal(song.toJSON());
            done();
          });
        });
      });
    });

    it('should update when calling save on the model and not call an extra findOne since we have the previous state', function (done) {
      var old = Song.findOne;
      var called = 0;

      Song.findOne = function () {
        called++;
        old.apply(Song, arguments);
      };

      Song.findOne({ id: id }, function (err, song) {
        assume(err).is.falsey();

        newOtherId = uuid.v4();
        song.otherId = newOtherId;

        song.save(function (err) {
          assume(err).is.falsey();
          Song.findOne({ id: id }, function (err, result) {
            assume(err).is.falsey();
            assume(result.toJSON()).to.deep.equal(song.toJSON());
            assume(called).to.equal(2);
            Song.findOne = old;
            done();
          });
        });
      });
    });

    it('should remove from all lookup tables', function (done) {
      Song.remove({
        id: id,
        otherId: newOtherId,
        uniqueId: uniqueId
      }, function (err) {
        assume(err).is.falsey();

        findOneAll({ otherId: newOtherId }, function (err, result) {
          assume(err).is.falsey();
          Object.keys(result).forEach(function (key) {
            assume(result[key]).is.falsey();
          });
          done();
        });
      });
    });

    it('should error when trying to remove without required attributes', function (done) {
      Song.remove({
        id: id,
        otherId: otherId
      }, function (err) {
        assume(err).to.be.instanceof(Error);
        done();
      });
    });
  });

  describe('foo', function () {
    var one = uuid.v4();
    var two = uuid.v4();
    var three = uuid.v4();
    var four = uuid.v4();
    var five = uuid.v4();
    var six = uuid.v4();
    var seven = uuid.v4();
    var eight = uuid.v4();

    var Foo;

    after(function (done) {
      if (Foo) return Foo.dropTables(done);
      done();
    });

    it('should create a table with an alter statement', function (done) {
      Foo = datastar.define('foo', {
        schema: schemas.foo,
        with: {
          compaction: {
            'class': 'LeveledCompactionStrategy'
          }
        }
      });

      Foo.ensureTables(function (err) {
        if (err) {
          console.error(err);
          return done(err);
        }
        done();
      });
    });

    it('should create multiple records in the database', function (done) {
      var next = assume.wait(2, 2, done);

      Foo.create({ fooId: one }, function (err) {
        assume(err).is.falsey();
        next();
      });

      Foo.create({ fooId: two }, function (err) {
        assume(err).is.falsey();
        next();
      });
    });

    it('should create a record in the database that will properly expire with given ttl', function (done) {
      Foo.create({ entity: { fooId: three }, ttl: 3 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: three }, function (error, res) {
          assume(error).is.falsey();
          assume(res);
          assume(res.fooId).equals(three);

          setTimeout(function () {
            Foo.findOne({ fooId: three }, function (er, result) {
              assume(er).is.falsey();
              assume(result).is.falsey();
              done();
            });
          }, 3000);
        });
      });
    });

    it('should create a record in the database that will expire but still be found before it reaches given ttl', function (done) {
      Foo.create({ entity: { fooId: four }, ttl: 7 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: four }, function (err, res) {
          assume(err).is.falsey();
          assume(res);
          assume(res.fooId).equals(four);

          setTimeout(function () {
            Foo.findOne({ fooId: four }, function (err, result) {
              assume(err).is.falsey();
              assume(result);
              assume(result.fooId).equals(four);
              done();
            });
          }, 3000);
        });
      });
    });

    it('should update a record in the database that will expire with given ttl', function (done) {
      Foo.update({ entity: { fooId: five, something: 'foo' }, ttl: 2 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: five }, function (error, result) {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(five);

          setTimeout(function () {
            Foo.findOne({ fooId: five }, function (er, res) {
              assume(er).is.falsey();
              assume(res).is.falsey();
              done();
            });
          }, 5000);
        });
      });
    });

    it('should update a record in the database that can be found before it reaches ttl', function (done) {
      Foo.update({ entity: { fooId: six, something: 'foo' }, ttl: 5 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: six }, function (error, result) {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(six);

          setTimeout(function () {
            Foo.findOne({ fooId: six }, function (er, res) {
              assume(er).is.falsey();
              assume(res);
              assume(res.fooId).equals(six);
              done();
            });
          }, 2000);
        });
      });
    });

    it('should update a record in the database with an updated reset ttl and can be found before it reaches the updated ttl', function (done) {
      Foo.update({ entity: { fooId: seven, something: 'boo' }, ttl: 3 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: seven }, function (error, result) {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(seven);

          Foo.update({ entity: { fooId: seven, something: 'foo' }, ttl: 10 }, function (error) {
            assume(error).is.falsey();

            setTimeout(function () {
              Foo.findOne({ fooId: seven }, function (er, res) {
                assume(er).is.falsey();
                assume(res);
                assume(res.fooId).equals(seven);
                done();
              });
            }, 5000);
          });
        });
      });
    });

    it('should update a record in the database with an updated reset ttl and expire after it reaches the updated ttl', function (done) {
      Foo.update({ entity: { fooId: eight, something: 'boo' }, ttl: 2 }, function (err) {
        assume(err).is.falsey();

        Foo.findOne({ fooId: eight }, function (error, result) {
          assume(error).is.falsey();
          assume(result);
          assume(result.fooId).equals(eight);

          Foo.update({ entity: { fooId: eight, something: 'foo' }, ttl: 3 }, function (error) {
            assume(error).is.falsey();

            setTimeout(function () {
              Foo.findOne({ fooId: eight }, function (er, res) {
                assume(er).is.falsey();
                assume(res).is.falsey();
                done();
              });
            }, 3000);
          });
        });
      });
    });

    it('should run a find query with a limit of 1 and return 1 record', function (done) {
      Foo.findAll({ conditions: {}, limit: 1 }, function (err, recs) {
        assume(err).is.falsey();
        assume(recs.length).equals(1);
        done();
      });
    });

    it('should remove entities', function (done) {
      var next = assume.wait(2, 2, done);
      Foo.remove({ fooId: one }, function (err) {
        assume(err).is.falsey();
        next();
      });

      Foo.remove({ fooId: two }, function (err) {
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
    }, function (err, res) {
      assume(err).is.falsey();
      callback(null, res);
    });
  }

  after(function (done) {
    datastar.close(done);
  });
});


