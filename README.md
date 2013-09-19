nosqlite.py
===========

``nosqlite.py`` is a pure python library that aims to provide a schemaless wrapper
for interacting with sqlite databases. Much of the behavior follows how the API
for [pymongo](http://api.mongodb.org/python/current) works, so those familiar with
that library should have a similar experience. Example::

```python
import nosqlite

with nosqlite.Connection(':memory:') as conn:
    foo_collection = conn['foo_collection']
    foo_collection.insert({'foo': 'bar', 'baz': 'qux'})
    foo_collection.find({'foo': 'bar'})
```


TODOs
-----
- Need to implement better queries. Currently there is an implicit AND query performed
  but it would be nice to have something like ``{'$gt': {'foo': 5}}``
- Indexes need to be implemented


Contribution and License
------------------------
Developed by Shaun Duncan <shaun.duncan@gmail.com> and is licensed under the
terms of a MIT license. Contributions are welcomed and appreciated.
