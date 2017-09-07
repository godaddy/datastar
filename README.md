# datastar

[![Build
Status](https://travis-ci.org/godaddy/datastar.svg?branch=master)](https://travis-ci.org/godaddy/datastar)

> "Now witness the power of this FULLY ARMED AND OPERATIONAL DATASTAR!"

```
npm install datastar --save
```

### Contributing

This module is open source! That means **we want your contributions!**

1. Install it and use it in your project.
2. Log bugs, issues, and questions [on Github](https://github.com/godaddy/datastar)
3. Make pull-requests and help make us it better!

<hr/>

- [Usage](#usage)
- [Warnings](#warnings)
- [API Documentation](#api-documentation)
  - [Defining Models](#define)
    - [consistency](#consistency)
  - [Schema Validation](#schema-validation)
  - [Lookup Tables](#lookup-tables)
  - [Model.create](#modelcreate)
  - [Model.find](#modelfind)
    - [Stream API](#stream-api)
  - [Model.remove](#modelremove)
  - [Model.update](#modelupdate)
  - [Model hooks](#model-hooks)
  - [Create Tables](#create-tables)
  - [Drop Tables](#drop-tables)
  - [Statement Building](#statement-building)
  - [Conventions: casing, pluarlization, etc.](#conventions)
- [Tests](#tests)
- [Contributors](#contributors)

## Usage

``` js
var Datastar = require('datastar');
var datastar = new Datastar({
  config: {
    user: 'cassandra',
    password: 'cassandra',
    keyspace: 'a_fancy_keyspace',
    hosts: ['127.0.0.1', 'host2', 'host3']
  }
}).connect();

var cql = datastar.schema.cql;

var Artist = datastar.define('artist', {
  schema: datastar.schema.object({
    artist_id: cql.uuid(),
    name: cql.text(),
    create_date: cql.timestamp({ default: 'create' }),
    update_date: cql.timestamp({ default: 'update' }),
    members: cql.set(cql.text()),
    related_artists: cql.set(cql.uuid()).allow(null),
    traits: cql.set(cql.text()),
    metadata: cql.map(cql.text(), cql.text()).allow(null)
  }).partitionKey('artist_id'),
  with: {
    compaction: {
      class: 'LeveledCompactionStrategy'
    }
  }
});

Artist.create({
  artistId: '12345678-1234-1234-1234-123456789012',
}, function (err, res) {
  if (err) /* handle me */ return;
  /* The create statement(s) have executed. */
});
```

## Warnings

* Define schemas in snakecase, however always use camelcase instead of snakecase everywhere else but schema definitions.

## API Documentation


### Constructor

All constructor options are passed directly to [Priam](https://github.com/godaddy/node-priam) so any options Priam supports, Datastar supports.

```js
var Datastar = require('datastar');
var datastar = new Datastar({
  config: {
    // who am I connecting as
    user: 'cassandra',
    // what's my password
    password: 'cassandra',
    // what keyspace am I using
    keyspace: 'a_fancy_keyspace',
    // what cluster hosts do I know about
    hosts: ['127.0.0.1', 'host2', 'host3']
  }
});
```

### Connect

Given a set of cassandra information, connect to the cassandra constructor. **This must be called before you do any model creation, finding, schema defintion, etc.**

```js
var datastar = new Datastar(...);
// I setup the connection, but I'm not connected yet, let's connect!
datastar = datastar.connect();
```

### Define

Define is the primary way to create `Models` while using Datastar. See the following long example for an explanation of all options to define. **Define your schemas in snake case, but use camel case everywhere else!**

```js

//
// main definition function, pass a name of the model (table), and it's corresponding schema
//
var Album = datastar.define('album', {
  //
  // ensure that the table exists. This executes an implicit CREATE IF NOT EXISTS
  // statement. You can listen for completio using the `ensure-tables:finished`
  // event
  ensureTables: true,
  //
  // the actual schema definition
  //
  schema: datastar.schema.object({
    album_id: cql.uuid(),
    artist_id: cql.uuid(),
    name: cql.text(),
    track_list: cql.list(cql.text()),
    song_list: cql.list(cql.uuid()),
    release_date: cql.timestamp(),
    create_date: cql.timestamp(),
    producer: cql.text()
  }).partitionKey('artist_id')
    .clusteringKey('album_id'),
  //
  // Support `with` extensions onto the table statement when calling .ensureTables
  //
  with: {
    //
    // key value of how you want to alter the table
    //
    compaction: {
      class: 'LeveledCompactionStrategy'
    },
    gcGraceSeconds: 9860,
    //
    // How we want to order our rows on disk for clustering keys
    //
    orderBy: {
      key: 'album_id',
      order: 'desc'
    }
  }
});
```

#### Consistency

Since Cassandra is a distributed database, we need a way to specify what our
consistency threshold is for both reading and writing from the database. We can
set `consistency`, `readConsistency` and `writeConsistency` when we define our
model. `consistency` is used if you want to set both to be the same threshold
otherwise you can go more granular with `readConsistency` and
`writeConsistency`. Consistency is defined using a `camelCase` string that
corresponds with a consistency that cassandra allows.

```js
var Album = datastar.define('album', {
  schema: albumSchema,
  readConsistency: 'localQuorum',
  writeConsitency: 'one'
});
```

We also support setting consistency on an operation basis as well if you want to
override the default set on the model for specific cases.

```js
Album.create({
  entity: {
    albumId: uuid.v4(),
    name: 'nevermind'
  },
  consistency: 'localOne'
}, function (err) {
  if (err) /* handle me */ return;
  /* create is completed */
});
```

We also support setting an optional expiration period called TTL(Time To Leave) for data expiration and removal. You can set up the TTL option either when creating the data entry or updating it. Once when you update the data entry, it will reset its TTL. 
```js
Album.create({
  entity: {
    albumId: uuid.v4(),
    name: 'nevermind'
  },
  ttl: 3000
}, function (err) {
  if (err) /* handle me */ return;
  /* create is completed */
});

OR

Album.update({
  entity: {
    albumId: uuid.v4(),
    name: 'whatever'
  },
  ttl: 5000
}, function (err) {
  if (err) /* handle me */ return;
  /* update is completed */
});

```

> Note: `ttl` can NOT be passed down from the previous existing data entry when calling `update`. So please always keep in mind to pass in the secondary ttl when updating the data entry, or it will default to NOT expired.

### Schema Validation

Schemas are **validated on .define**. As you call each function on a `Model`,
such as `create` or `find`, the calls to the functions are validated against the
schema. [See here](https://github.com/godaddy/joi-of-cql) for detailed information of supported CQL data types.

Validation is performed using [joi](https://github.com/hapijs/joi).

Notes on `null`:
  - Use the `.allow(null)` function of `joi` on any property you want to allow
  to be null when creating your schema

The following table show how certain data types are validated:

CQL Data Type | Validation Type
------------  | -------------
`ascii`       | `cql.ascii()`
`bigint`      | `cql.bigint()`
`blob`        | `cql.blob()`
`boolean`     | `cql.boolean()`
`counter`     | `cql.counter()`
`decimal`     | `cql.decimal()`
`double`      | `cql.double()`
`float`       | `cql.float()`
`inet`        | `cql.inet()`
`text`        | `cql.text()`
`timestamp`   | `cql.timestamp()`
`timeuuid`    | `cql.timeuuid()`
`uuid`        | `cql.uuid()`
`int`         | `cql.int()`
`varchar`     | `cql.varchar()`
`varint`      | `cql.varint()`
`map`         | `cql.map(cql.text(), cql.text())`,
`set`         | `cql.set(cql.text())`


### Lookup tables

This functionality that we built into `datastar` exists in order to optimize queries for other unique keys on your main table. By default Cassandra has the ability to do this for you by building an index for that key. The only problem is that the current backend storage of Cassandra can make these very slow and under performant. If this is a high traffic query pattern, this could lead you to having issues with your database. We work around this limitation by simply creating more tables and doing an extra write to the database. Since Cassandra is optimized for handling a heavy write workload, this becomes trivial. We take care of the complexity of keeping these tables in sync for you. Lets look at an example by modifying our `Artist` model.

```js
var Artist = datastar.define('artist', {
  schema: datastar.schema.object({
    artist_id: cql.uuid(),
    name: cql.text(),
    create_date: cql.timestamp({ default: 'create' }),
    update_date: cql.timestamp({ default: 'update' }),
    members: cql.set(cql.text()),
    related_artists: cql.set(cql.uuid()).allow(null),
    traits: cql.set(cql.text()),
    metadata: cql.map(cql.text(), cql.text()).allow(null)
  }).partitionKey('artist_id'),
    .lookupKeys('name')
  with: {
    compaction: {
      class: 'LeveledCompactionStrategy'
    }
  }
});

 ```

In our example above we added `name` as a `lookupKey` to our `Artist` model. This means a few things:

1. We must provide a `name` when we create an `Artist`.
2. `name` as with any `lookupKey` __MUST__ be unique
3. We must provide the `name` when removing an `Artist` as it is now the
   primary key of a different table.
4. When updating an `Artist`, a fully formed `previous` value must be given or
   else an implicit `find` operation will happen in order to properly assess if
   a `lookupKey` has changed.

Keeping these restrictions in mind, we can now have fast lookups by `name` without having to worry about too much.

```js
Artist.findOne({
  name: 'kurt cobain'
}, function (err, artist) {
  if (err) /* handle me */ return;
  console.log('Fetched artist by name!');
});
```


### Model.create

Once you have created a `Model` using `datastar.define` you can start creating records against the Cassandra database you have configured in your options or passed to `datastar.connect`:

``` js
var cql = datastar.schema.cql;

var Beverage = datastar.define('beverage', {
  schema: datastar.schema.object({
    'beverage_id': cql.uuid({ default: 'v4' }),
    'name': cql.text(),
    'type': cql.text().allow(null),
    'sugar': cql.int(),
    'notes': cql.text(),
    'otherIngredients': cql.map(cql.text(), cql.text()),
    'tags': cql.set(cql.text()),
    'siblings': cql.set(cql.uuid())
  }).partitionKey('beverage_id'),
});

Beverage.create({
  name: 'brawndo',
  sugar: 1000000,
  notes: "It's got what plants crave"
}, function (err) {
  if (err) /* handle me */ return;
  /* create is completed */
});
```

The `create` method (like all CRUD methods) will accept four different arguments for convenience:

``` js
// Create a single model with properties
Model.create(properties);
Model.create({ entity: properties });
Model.create({ entities: properties });

// Create a two models: one with properties
// and the second with properties2
Model.create({ entities [properties, properties2] })

```

### Model.update

Updating records in the database is something that is fairly common. We expose a
simple method to do this where you just provide a partial object representing
your `Beverage` and it will figure out how to update all the fields! Lets see what it
looks like.

```js
//
// For the simple case update some basic field
//
Beverage.update({
  name: 'brawndo',
  sugar: 900000000,
  notes: "It's got what plants crave, now with more sugar!",
  //
  // This value will get merged in with whatever keys currently exist for this
  // `map`
  //
  otherIngredients: {
    energy: '9001'
  },
  //
  // This replaces the set that currently exists in the database with these
  // values
  //
  tags: ['healthy', 'energy', 'gives you wings']
}, function (err) {
  if (err) /* handle me */ return;
  /* update is completed */
});
```

It even supports higher level functions on `set` and `list` types. Lets look at what
`set` looks like. (`list` covered father down)

```js
Beverage.update({
  name: 'brawndo',
  //
  // The keys of this object are the `actions` that can be made on the set in
  // cassandra. This allows us to understand which CQL to be sent when updating.
  //
  tags: {
    add: ['amazing', 'invincible'],
    remove: ['gives you wings']
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update success */
});
```

If we decide to create a model that needs to use `Lookup Tables`, we require a
`previous` value to be passed in as well as the `entity` being `updated`. If no
`previous` value is passed in, we will implicitly run a `find` on the primary
table to get the latest record before executing the update. This `previous`
value is required because we have to detect whether a `primaryKey` of a lookup
table has changed.

 __IMPORTANT__:
 > If you have a case where you are modifying the `primaryKey` of a
 > lookup table and you are PASSING IN the previous value into the `update`
 > function, that `previous` value MUST be a fully formed object of the previous
 > record, otherwise you are guaranteed to have the changed lookup table go out of
 > sync. Passing in your own previous value is done at your own risk if you do not
 > understand this warning or the implications, please post an issue.

```js
var Person = datastar.define('person', {
  ensureTables: true,
  schema: datastar.schema.object({
    person_id: datastar.schema.cql.uuid({ default: 'v4' }),
    // We are assuming names are unique
    name: datastar.schema.cql.text(),
    characteristics: datastar.schema.cql.list(datastar.schema.cql.text()),
    attributes: datastar.schema.cql.map(
      datastar.schema.cql.text(),
      datastar.schema.cql.text()
    )
  }).rename('id', 'person_id')
    //
    // Create lookup table so I can look up by `name` as well since it is unique
    //
    .lookupKeys('name')
});

//
// person Object
//
var person = {
  name: 'Fred Flinstone',
  attributes: {
    height: '6 foot 1 inch'
  }
};

//
// So we start but creating a person
//
Person.create(person, function (err) {
  if (err) /* handle me */ return;
  /* person created */
});

//
// Now if I later want to update this same and change the name... I need to pass
// in that FULL person object that was used previously. I will warn again that
// DATA WILL BE LOST if `previous` is an incomplete piece of data. This means that the `person_by_name`
// lookup table that gets generated under the covers will have incomplete data.
//
Person.update({
  previous: person,
  entity: {
    name: 'Barney Rubble',
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed */
});

//
// If I want to update the primary key of the entity I would need to do something as follows
// (this also shows changing the lookup table primary key at the same time)
//

Person.remove(previous, function(err) {
  if (err) /* handle me */ return;
  previous.name = 'Barney Rubble';
  previous.personId = '12345678-1234-1234-1234-123456789012'

  Person.create(previous, function (err) {
    if (err) /* handle me */ return;
    /* successful create */
  });
});

//
// If I ommit previous altogether, I just need to ensure I pass in the proper
// primary key for the `person` table and a find operation will be done in order to
// fetch the previous entity. This is the simplest method but costs an implicit
// find operation before the update is completed.
//
Person.update({
  id: person.id,
  name: 'Barney Rubble',
  attributes: {
    hair: 'blonde'
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed */
});

//
// Just like `set` types, we can do higher level operations with `list` types.
// Lets start by updateing a list
//
Person.update({
  id: person.id,
  characteristics:['fast', 'hard working']
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed */
});

//
// We can place items on the front of the list using `prepend`
//
Person.update({
  id: person.id,
  characteristics: {
    prepend: ['lazy']
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed, characteristics = ['lazy', 'fast', 'hard working'] */
});

//
// We can also add them to the end of the list as well with `append`
//
Person.update({
  id: person.id,
  characteristics: {
    append: ['helpful']
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed, characteristics = ['lazy', 'fast', 'hard working', 'helpful'] */
});

//
// We also have a standard `remove` operation that can be done
//
Person.update({
  id: person.id,
  characteristics: {
    remove: ['fast']
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed, characteristics = ['lazy', 'hard working', 'helpful'] */
});

//
// The last function we support on `list` is the `index` operation. It replaces
// the item in the `list` at the given index with the value associated.
//
Person.update({
  id: person.id,
  characteristics: {
    //
    // Replaces the item at index 1.
    //
    index: { '1': 'disabled' }
  }
}, function (err) {
  if (err) /* handle me */ return;
  /* update completed, characteristics = ['lazy', 'disabled', 'helpful'] */
});
```

### Model.find

Querying Cassandra can be the source of much pain, which is why `datastar` will only allow queries on models based on primary keys. Any post-query filtering is the responsibility of the consumer.

There are four variants to `find`:

- `Model.find(options, callback)`
- `Model.findOne(options || key, callback)` <-- Also has an alias `Model.get`
- `Model.findFirst(options, callback)`
- `Model.count(options, callback)`

The latter three (`findOne/get`, `findFirst`, and `count`) are all facades to `find` and simply do not need the `type` parameter in the example below.
Another note here is that `type` is implied to be `all` if none is given.
```js
Album.find({
  type: 'all',
  conditions: {
    artistId: '00000000-0000-0000-0000-000000000001'
  }
}, function (err, result) {
  if (err) /* handle me */ return;
  /* `result` will be an array of instances of `Album` */
});
```

In the latter three facades, you can also pass in `conditions` as the options
object!

```js
Album.findOne({
  artistId: '00000000-0000-0000-0000-000000000001',
  albumId: '00000000-0000-0000-0000-000000000005'
}, function (err, result) {
  if (err) /* handle me */ return;
  /* `result` will be an instance of `Album` */
});
```

You only need to pass a separate conditions object when you want to add
additional parameters to the query like `LIMIT`. Limit allows us to limit how
many records we are retrieving for any range query

```js
Album.findAll({
  conditions: {
    artistId: '00000000-0000-0000-0000-000000000001'
  },
  limit: 1
}, function (err, results) {
  if (err) /* handle me */ return;
  /* results.length === 1 */
});
```

We can also just pass in a single key in order to fetch our record. **NOTE**
This only works if your schema only has a single partition/primary key and
assumes you are passing in that key. This will not work for lookup tables.

```js
Artist.get('00000000-0000-0000-0000-000000000001', function(err, result) {
  if (err) /* handle me */ return;
  /* `result` will be an instance of `Artist` */
});
```

#### Stream API

While also providing a standard callback API, the find function supports first
class streams! This is very convenient when doing a `findAll` and processing
those records as they come instead of waiting to buffer them all into memory.
For example, if we were doing this inside a request handler:

```js
var through = require('through2');

//
// Fetch sodas handler
//
function handler(req, res) {
  Album.findAll({
    artistId: '00000000-0000-0000-0000-000000000001'
  })
  .on('error', function (err) {
    // Handle me for when something bad happens
    res.writeHead(500);
    res.end(JSON.stringify({ error: err.message }));
  })
  .pipe(through.obj(function (bev, enc, callback) {
    //
    // Massage the beverage object in some way before returning it to the user
    //
    callback(null, massageBeverage(bev));
  }))
  .pipe(res);
}
```

### Model.remove

If you would like to remove a record or a set of records from the database, you
just need to pass in the right set of `conditions`.

- `Model.remove(options, callback);`

One thing to note here is that when deleting a single record, you can pass in
the fully formed object and we will handle stripping the unsafe parameters that
you cannot query upon based on your defined schema.

```js
Album.remove({
  artistId: '00000000-0000-0000-0000-000000000001',
  albumId: '00000000-0000-0000-0000-000000000005'
  // any other properties.
}, function (err) {
  if (err) /* handle me */ return;
  console.log('Successfully removed beverage');
});
```

This also works on a range of values, given a schema with `artistId` as the
partition key and `albumId` as the clustering key, like our `album` model at the top of
this readme...
```js
//
// This will delete ALL elements for a given `artistId`
//
Album.remove({
  artistId: '00000000-0000-0000-0000-000000000001'
}, function (err) {
  if (err) /* handle me */ return;
  console.log('Successfully removed all albums');
});
```

When you remove an entity that has an associated lookup table, you need to pass
in both the **partition keys of the main table AND the lookup table**.

```js
//
// Going back to our `person` model..
//
Person.remove({
  personId: '12345678-1234-1234-1234-123456789012',
  name: 'steve belaruse'
}, function(err) {
  if (err) /* handle me */ return;
  console.log('Successfully removed artist');
});
```

This is necessary because a `lookupKey` defines the partition key of a different
table that is created for lookups.

### Model hooks

Arguably one of the most powerful features hidden in `datastar` are the model hooks or life-cycle events
that allow you to hook into and  modify the execution of a given statement. First let's define the operations and the hooks that they have associated.


|          Operation                  | Life-cycle event / model hook   |
|-------------------------------------|---------------------------------|
|create, update, remove, ensure-tables|  build, execute                 |
| find                                |  all, count, one, first         |

`datastar` utilizes a module called [`Understudy`][understudy] under the hood.
This provides a way to add extensibility to any operation you perform on a
specific `model`. Lets take a look at what this could look like.

```js
//
// Before build is before we create the `statement(s)` that we then collect to
// execute and insert into Cassandra. This allows us to modify any of the
// entities before CQL is generated for them
//
Beverage.before('create:build', function (options, callback) {
  //
  // We create our own option that gets passed in by the beverage option that
  // multiplies all number values by the given multiple. Might not be
  // practically useful but serves the point of how we can manipulate objects
  // conditionally in our application by leveraging `datastar`'s machinery.'
  //
  if (options.multiply) {
    //
    // Here we can modify the entities that were passed in. If a single entity
    // was passed in, this will still be an array, just of length 1
    //
    options.entities = options.entities.map(function (ent) {
      return Object.keys(ent).reduce(function (acc, key) {
        acc[key] = typeof ent[key] === 'number'
          ? ent[key] * options.multiply
          : ent[key];

        return acc;
      }, {})
    });
  }
  //
  // We even have a callback interface here in case we have to make an
  // asynchronous call to another service or database.
  //
  callback();
});

//
// Before execute is right before we actually send the statements to cassandra!
// This is where we have a chance to modify the statements or
// `StatementCollection` with any other statements we may have or even to just
// the `consistency` we are asking of cassandra if there is only a narrow case
// where you require a consistency of `one`. (You could also just pass
// option.consistency into the function call as well)
//
Beverage.before('create:execute', function (options, callback) {
  if (options.commitFast) {
    options.statements.consistency('one');
    //
    // Execute all statements in parallel with a limit of 7
    //
    options.statements.strategy = 7;
  }


  callback();
});

//
// An `after` hook might for `execute` might look like this if we wanted to
// insert the same data into a separate keyspace using a different `Priam`
// instance. Which would be a separate connection to cassandra. This call is
// ensured to be executed before the `Beverage.create(opts, callback)` function
// calls its callback.
// NOTE: This assumes the same columns exist in this other keyspace
//
var otherDataCenterConnection = new Priam(connectOpts);
Beverage.after('create:execute', function (options, callback) {
  //
  // Reuse the statement collection from the create operation to execute the
  // same set of statements on another connection
  //
  options.statements.connection = otherDataCenterConnection;

  options.statements.execute(callback);
});

//
// The last type of hook we have is for the specific `find` operations
// including. `find:all`, `find:one`, `find:count`, `find:first`. These specifc
// are the same as the above `:build` hooks in when they execute but have
// different and more useful semantics for `after` hooks for modifying data
// fetched. This makes use of [`Understudy's`][understudy] `.waterfall`
// function.
//

//
// The after hooks on `find:one` allow us to mutate the result returned from any
// `findOne` query taken on beverage. This could allow us to call an external
// service to fetch extra properties or anything else you can think of. The main
// goal is to provide the extensibility to do what you want without `datastar`
// getting in your way.
//
Beverage.after('find:one', function (result, callback) {
  //
  // Populate associated sibling models on every `findOne` or `get` query
  //
  async.map(result.siblings,
    function (id, next) {
    Beverage.get(id, next);
  }, function (err, siblingModels) {
    if (err) { return callback(err); }
    result.siblings = siblingModels;
    callback()
  });
});

//
// We can even add another after hook after this one which will get executed in
// series and be able to modify any new attributes!
//

Beverage.after('find:one', function (result, callback) {
  //
  // Now that siblings are populated, modify their siblings if they arent
  // properly associated with their sibling
  //
  async.each(result.siblings, function (bev, next) {
    if (bev.siblings.indexOf(result.beverageId) !== -1) {
      return next();
    }
    //
    // TODO: Have instance functions for this type of thing
    //
    var update = bev.toJSON();
    //
    // Do the more efficient update to cassandra on the `set` type.
    //
    update.siblings = {
      add: [result.beverageId]
    };
    bev.siblings.push(result.beverageId);
    Beverage.update(update, function (err) {
      if (err) { return next(err); }
      next(null, bev);
    });
  }, function (err, res) {
    if (err) { return callback(err); }
    result.siblings = res;
    callback();
  });

});

```

### Create tables

Each `Model` is capable of creating the Cassandra tables associated with its `schema`.
To ensure that a table is created you can pass the `ensureTables` option:

```js
var Spice = datastar.define('spice', {
  ensureTables: true,
  schema: /* a valid schema */
})
```

Or call `Model.ensureTables` whenever is appropriate for your application:

```js
Spice.ensureTables(function (err) {
  if (err) /* handle me */ return;
  console.log('Spice tables created successfully.');
});
```

You can also specify an `with.orderBy` option to enable `CLUSTER ORDER BY` on a
partition key. This is useful if you want your `Spice` table to store the newest
items on disk first, making it faster to reading in that order.

```js
//
// WITH CLUSTER ORDER BY (created_at, DESC)
//

Spice.ensureTables({
  with: {
    orderBy: { key: 'createdAt', order: 'DESC' }
  }
}, function (err) {
  if (err) /* handle me */ return;
  console.log('Spice tables created ordered descending');
})
```

//
// We can also pass an option to enable setting other properties of a table as well!
//
```js
Spice.ensureTables({
  with: {
    compaction: {
      class: 'LeveledCompactionStrategy',
      enabled: true,
      sstableSizeInMb: 160,
      tombstoneCompactionInterval '86400'
    },
    gcGraceSeconds: 86400
  }
}, function (err) {
  if (err) /* handle me */ return;
  console.log('Successfully created and altered the table');
});

```

### Drop Tables

In `datastar`, each model also has the ability to drop tables. This assumes the
user used to establish the connection has these permissions. Lets see what this
looks like with our `spice` model.

```js
Spice.dropTables(function (err) {
  if (err) /* handle me */ return;

  console.log('Spice tables dropped!');
});

```

Its as simple as that. We will drop the spice table and any associated Lookup
Tables if they were configured. With `.dropTables` and `.ensureTables` it's
super easy to use `datastar` as a building block for managing all of your Cassandra
tables without executing any manual `CQL`.

### Statement Building

Currently this happens within the `model.js` code and how it interacts with the
`statement-builder` and appends statements to the `StatementCollection`.
Currently the best place to learn about this is read through the
`create/update/remove` pathway and follow how a statement is created and then
executed. In the future we will have comprehensive documentation on this.

### Conventions

We make a few assumptions which have manifested as conventions in this library.

1. We do not store `null` values in cassandra itself for an `update` or `create`
   operation on a model. We store a `null` representation for the given
   cassandra type. This also means that when we fetch the data back from
   cassandra, we return the data back to you with the proper `null`s you would
   expect. It just may be unintuitive if you look at the cassandra tables
   directly and do not see `null` values.

   This prevents tombstones from being created which has been crucial for our
   production uses of cassandrai at GoDaddy. This is something that could be configurable
   in the future.

2. Casing, as mentioned briefly in the warning at the top of the readme, we assume
   `camelCase` as the casing convention when interacting with datastar and the
   models created with it. The schema is the only place where the keys used MUST
   be written as `snake_case`.

## Tests

Tests are written with `mocha` and code coverage is provided with `istanbul`. They can be run with:

```
# Run all tests with "pretest"
npm test

# Just run tests
npm run coverage
```

## Contributors

- [Christopher Jeffrey](https://github.com/chjj)
- [Sam Shull](https://github.com/samshull)
- [Jarrett Cruger](https://github.com/jcrugzz)
- [Fady Matar](https://github.com/fmatar)
- [Charlie Robbins](https://github.com/indexzero)
- [Adrian Chang](https://github.com/amchang)
- [Troy Rhinehart](https://github.com/gingur)
- [Steve Commisso](https://github.com/scommisso)
- [Joe Junker](https://github.com/JosephJNK)
- [Bill Enterline](https://github.com/enterline)

[understudy]: https://github.com/bmeck/understudy
