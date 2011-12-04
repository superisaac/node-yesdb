# node-yesdb

nodejs implementation of bitcask log-based key-value storage

author: Zeng Ke
email: superisaac.ke@gmail.com

## License
node-yesdb is licensed under MIT license

## Features

* all operations, open/set/put, are asynchronized
* data files is text based, so some some unix command line tools can
  be used to view or opeate on them.

## Example

```javascript
var yesdb = require('./yesdb');

var db = yesdb.opendb('xxx');

// this operation will not run until opening database is completed
db.set('abc', 'defx神马', function () {
	console.info('set item ok');
    });

// this operation will not run until opening database is completed
db.get('abc', function (err, v) {
	console.info('get value', v);
    });

console.log('This statement should be executed before db operations');
```

## TODO:

* collect data files to reduce the number
* hint files
* delete key
* others ....


