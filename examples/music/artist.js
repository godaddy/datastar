'use strict';

module.exports = function (datastar, models) {
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
    readConsistency: 'one',
    writeConsistency: 'localQuorum',
    with: {
      compaction: {
        class: 'LeveledCompactionStrategy'
      }
    }
  });

  return Artist;
};
