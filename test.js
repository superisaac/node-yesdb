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
