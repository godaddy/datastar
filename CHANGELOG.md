# Changelog

## 4.0.0

Breaking:
- Support for node 8 is dropped; minimum node version is now 10.17.x.
- The configuration schema is now aligned with `priam` version 4. See the [`priam` migration guide](https://github.com/godaddy/node-priam/blob/master/MIGRATION.md) for help with converting your config settings.

Features:
- Data can now be queried as an async iterable for a more lightweight alternative to streams.
- Assigning a `transform` function to your model now gives you the ability to convert all queried records regardless of whether you used the callback or streaming interface.

Internals:
- Data processing has been streamlined.
