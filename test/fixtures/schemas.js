

var joi = require('joi-of-cql');

var cql = joi.cql;

/**
 * @property schemas {Object}
 */
module.exports = {
  artist: joi.object({
    artist_id: cql.uuid(),
    name: cql.text(),
    create_date: cql.timestamp(),
    update_date: cql.timestamp(),
    members: cql.set(cql.text()),
    related_artists: cql.set(cql.uuid()).allow(null),
    traits: cql.set(cql.text()),
    metadata: cql.map(cql.text(), cql.text())
  }).partitionKey('artist_id')
    .rename('id', 'artist_id', { ignoreUndefined: true }),
  album: joi.object({
    artist_id: cql.uuid(),
    album_id: cql.uuid(),
    name: cql.text(),
    track_list: cql.list(cql.text()),
    song_list: cql.list(cql.uuid()),
    release_date: cql.timestamp(),
    create_date: cql.timestamp(),
    update_date: cql.timestamp(),
    producer: cql.text()
  }).partitionKey('artist_id')
    .clusteringKey('album_id')
    .rename('id', 'album_id', { ignoreUndefined: true }),
  song: joi.object({
    song_id: cql.uuid(),
    unique_id: cql.uuid(),
    other_id: cql.uuid(),
    name: cql.text(),
    length: cql.text(),
    artists: cql.set(cql.uuid())
  }).partitionKey('song_id')
    .rename('id', 'song_id', { ignoreUndefined: true }),
  person: joi.object({
    person_id: cql.uuid(),
    name: cql.text(),
    create_date: cql.timestamp(),
    characteristics: cql.list(cql.text())
  }).partitionKey('person_id').rename('id', 'person_id', { ignoreUndefined: true }),
  cat: joi.object({
    cat_id: cql.uuid(),
    hash: cql.int(),
    name: cql.text(),
    create_date: cql.timestamp()
  }).partitionKey(['cat_id', 'hash']).rename('id', 'cat_id', { ignoreUndefined: true }),
  dog: joi.object({
    id: cql.uuid(),
    name: cql.text().disallow(null).min(2),
    color: cql.text(),
    weight: cql.int(),
    owner: cql.json(),
    vaccinations: cql.list(cql.json()),
    dog_thing: cql.text()
  }).partitionKey('id'),
  foo: joi.object({
    foo_id: cql.uuid(),
    something: cql.text()
  }).partitionKey('foo_id')
};

