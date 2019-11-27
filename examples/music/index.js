'use strict';

const http = require('http');
const url = require('url');
const qs = require('querystring');
const Datastar = require('datastar');
const stringify = require('stringify-stream');

//
// Setup connection and instances, it will lazily connect since
// we are just using a simple http server
//
const datastar = new Datastar({
  config: require('./config')
}).connect();

const models = require('./models')(datastar);

const stringifyOpts = { open: '[', close: ']' };

http.createServer(function respond(req, res) {
  const parsed = url.parse(req.url);
  const params = qs.parse(parsed.query);

  //
  // Naive routes that just return all of a given resource
  // to keep it simple
  //
  if (/^\/album/.test(parsed.path)) {
    res.writeHead(200, {
      'content-type': 'application/json',
      'Trailer': 'Error'
    });

    return models.Album.findAll({
      albumId: params.albumId
    })
    .once('error', writeTrailers(res))
    .pipe(stringify(stringifyOpts))
    .pipe(res, { end: false })
    .on('finish', () => res.end())
  }

  if (/^\/artist/.test(parsed.path)) {
    res.writeHead(200, {
      'content-type': 'application/json',
      'Trailer': 'Error'
    });

    return models.Artist.findAll({})
      .once('error', writeTrailers(res))
      .pipe(stringify(stringifyOpts))
      .pipe(res, { end: false })
      .on('finish', () => res.end());
  }

  res.writeHead(404, {
    'content-type': 'application/json'
  });
  res.end(JSON.stringify({ error: 'No Resource found' }));

}).listen(3000);

function writeTrailers(res) {
  return (err) => {
    res.addTrailers({ 'Error': err.message });
    res.end();
  }
};

