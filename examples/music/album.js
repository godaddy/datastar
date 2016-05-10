'use strict';

module.exports = function (datastar, models) {
  var cql = datastar.schema.cql;

  var Album = datastar.define('album', {
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
      .clusteringKey('album_id')
  });

  return Album;
};
