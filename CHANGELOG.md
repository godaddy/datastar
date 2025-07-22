# Changelog

## 4.0.4

- (fix) address vulnerable packagse

## 4.0.3

- (fix) address vulnerable packages

## 4.0.1

- (fix) bring back the publishing of `datastar/test` libraries used in some consumers' unit tests.

## 4.0.0

- (breaking) support for node 8 is dropped; minimum node version is now 10.17.x.
- (breaking) the configuration schema is now aligned with `priam` version 4. See the [`priam` migration guide](https://github.com/godaddy/node-priam/blob/master/MIGRATION.md) for help with converting your config settings.
- (feature) data can now be queried as an async iterable for a more lightweight alternative to streams.
- (feature) assigning a `transform` function to your model now gives you the ability to convert all queried records regardless of whether you used the callback or streaming interface.
- (fix) data processing has been streamlined.
