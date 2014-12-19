nosqlite.py
===========

``nosqlite.py`` is a pure python library for python 2 and 3 (2.6, 2.7, 3.3, and 3.4)
that aims to provide a schemaless wrapper for interacting with sqlite databases.
Much of the behavior follows how the API for [pymongo](http://api.mongodb.org/python/current)
works, so those familiar with that library should have a similar experience. Example::

```python
import nosqlite

with nosqlite.Connection(':memory:') as conn:
    foo_collection = conn['foo_collection']
    foo_collection.insert({'foo': 'bar', 'baz': 'qux'})
    foo_collection.find({'foo': 'bar'})
```


TODOs
-----
- Indexes need to be implemented and associated query planning
- Support for embedded documents and queries on those (i.e. {'foo.bar': 5})


Contribution and License
------------------------
Developed by Shaun Duncan <shaun.duncan@gmail.com> and is licensed under the
terms of a MIT license. Contributions are welcomed and appreciated.
