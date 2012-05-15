/**
 * node-yesdb: nodejs implementation of bitcask log-based key-value storage
 * Author: Zeng Ke
 * Email: superisaac.ke@gmail.com
 * 
 * node-yesdb is licensed under MIT license
 */
var fs = require('fs');
var EventEmitter = require('events').EventEmitter

function createLineStream(filename) {
    var emitter = new EventEmitter();
    var stream = fs.createReadStream(filename, {
	    encoding: 'utf-8'
	});
    var remain = '';
    var pos = 0;

    function handleRemain(succeed) {
	var idx = remain.indexOf('\n');
	while(idx >= 0) {
	    var line = remain.substr(0, idx + 1);
	    emitter.emit('line', line, pos);
	    pos += Buffer.byteLength(line);
	    remain = remain.substr(idx + 1);
	    idx = remain.indexOf('\n');
	}
	if(typeof succeed == 'function') {
	    succeed();
	}
    }
    
    stream.on('data', function (data) {
	    remain += data;
	    handleRemain();
	});
    
    stream.on('end', function () {
	    handleRemain(function () {
		    if(remain) {
			emitter.emit('line', remain, pos);
			pos += Buffer.byteLength(remain);
			remain = '';
		    }
		    emitter.emit('end', pos);
		});
	});
    return emitter;
}
    
function createItemStream(filename) {
    var stream = createLineStream(filename);
    stream.on('line', function (line, pos) {
	    line = line.trim();
	    var items = line.split('\t', 3);
	    var num = parseInt(items[1]);
	    // TODO: vanity check num
	    if(num == 0) {
		stream.emit('delitem', items[0], pos);
	    } else {
		stream.emit('item', items[0], items[2], pos);
	    }
	});
    return stream;
}

function opendb(dbpath, opts) {
    if(typeof opts == 'function') {
	opts = {'opened': opts};
    }
    opts = opts || {};
    var datafile_limit = opts.datafile_limit || 1024 * 1024 * 20;

    // Inner data structures
    var curr_pos = 0;
    var curr_filename;
    var curr_fd;

    var cache = new Object();
    var opened_fds = {};

    var ready_evt = new EventEmitter();
    var state = 'init';
    var hooks = [];
    var fnopened = opts.opened || function (){};

    // Interface objects providing methods: get/set
    var instance = {};
    function ready(fn) {
	if(state != 'opened') {
	    hooks.push(fn);
	} else {
	    fn(false);
	}
    }

    ready_evt.on('hookReady', function () {
	    if(hooks.length > 0) {
		var fn = hooks[0];
		hooks.shift();
		setTimeout(function() {fn(true);}, 0);
	    }
	});

    function readDataDir(fndataok) {
	fs.readdir(dbpath, function (err, files) {
		var datafiles = [];
		files.sort().forEach(function (f) {
			if(!/^cask_\d+\.dat$/.test(f)) {
			    return;
			}
			var datapath = dbpath + '/' + f;
			datafiles.push(datapath);			
		    });

		var remain_data = datafiles.length;
		if(remain_data > 0) {
		    datafiles.forEach(function (f) {
			    curr_filename = f;
			    readDataFile(f, function() {
				    remain_data--;
				    if(remain_data <=0) {
					fndataok();
				    }
				});
			});
		} else {
		    var timestamp = new Date().getTime();
		    curr_filename = dbpath + '/cask_' + timestamp + '.dat';
		    fndataok();
		}
	    });
    }

    function readDataFile(datafile, fnok) {
	var stream = createItemStream(datafile);
	stream.on('item', function (k, v, pos) {
		cache[k] = [datafile, pos];
	    });

	stream.on('delitem', function (k, pos) {
		delete cache[k];
	    });
	
	stream.on('error', function (err) {
		console.error(err);
	    });
	
	stream.on('end', function () {
		fnok();
	    });
    }
    
    function ensureDbPath(fnok) {
	fs.lstat(dbpath, function (err, stats) {
		if(err && err.code == 'ENOENT') { // No such dir, try to create one
		    fs.mkdir(dbpath, 0744, function (err) {
			    if(err) {
				console.error(err);
				process.exit(1);
			    }
			    fnok();
			});
		} else if(err) {
		    console.error(err);
		    process.exit(1);
		} else if(!stats.isDirectory()){
		    console.error(dbpath, 'is not directory');
		    process.exit(1);
		} else {
		    fnok();
		}
	    });
    }

    function initialize() {
	ensureDbPath(function () {
		readDataDir(function () {
			state = 'opened';
			fnopened(instance);
			ready_evt.emit('hookReady');
		    });
	    });
    }
    
    instance.set = function(key, value, fnok) {
	fnok = fnok || function () {};
	ready(function (as_hook) {
		function writeItem(fd) {
		    var evlen = 0;
		    var ev = '';			
		    if(value != undefined && value != null) {
			ev = JSON.stringify(value);
			evlen = Buffer.byteLength(ev, 'utf-8');
		    }
		    var s = encodeURIComponent(key) + '\t' + evlen + '\t' + ev + '\n';
		    var lens = Buffer.byteLength(s);
		    var buf = new Buffer(s);
		    cache[key] = [curr_filename, curr_pos];
		    curr_pos += lens;
		    fs.write(fd, buf, 0, lens, null, fnok);
		    if(as_hook) {
			ready_evt.emit('hookReady');
		    }
		    if(curr_pos > datafile_limit) {
		    }
		}

		function ensureCurrentFd(fnok) {
		    function rotateData() {
			if(curr_pos >= datafile_limit) {
			    var timestamp = new Date().getTime();
			    curr_filename = dbpath + '/cask_' + timestamp + '.dat';
			    console.log('rotate to', curr_filename);
			    fs.open(curr_filename, 'a+', function (err, fd) {
				    curr_fd = fd;
				    fnok();
				});
			} else {
			    fnok();
			}
		    }

		    if(!curr_fd) {
			fs.open(curr_filename, 'a+', function (err, fd) {
				curr_fd = fd;
				fs.fstat(fd, function (err, stats) {
					curr_pos = stats.size;
					rotateData();					    
				    });
			    });
		    } else {
			rotateData();
		    }
		}

		ensureCurrentFd(function() {
			writeItem(curr_fd, key, value);
		    });
	    });
    };

    instance.get = function(key, fnok) {
	fnok = fnok || function () {};
	ready(function (as_hook) {
		var pos = cache[key];
		if(pos == undefined) {
		    fnok('notfound');
		    if(as_hook) {
			ready_evt.emit('hookReady');
		    }
		} else {
		    readItem(pos, function(err, k, v) {
			    if(err) {
				fnok(err, null);
				return;
			    }
			    fnok(null, v);
			    if(as_hook) {
				ready_evt.emit('hookReady');
			    }
			});
		}
	    });
    };

    function readItem(filepos, callback) {
	var filename = filepos[0];
	var pos = filepos[1];
	function readLine(fd) {
	    // TODO: make the size adaptable.
	    var buffer = new Buffer(1024);
	    fs.read(fd, buffer, 0, 1024, pos, function (err, bytesRead) {
		    var s = buffer.toString('utf-8', 0, bytesRead);
		    var keyEndIndex = s.indexOf('\t');
		    if(keyEndIndex >= 0) {
			var key = decodeURIComponent(s.substr(0, keyEndIndex).trim());
			keyEndIndex++;
			var numIndex = s.indexOf('\t', keyEndIndex);
			if(numIndex < 0) {
			    callback('Illegal item position, cannot find number');
			    return;
			}
			var num = parseInt(s.substr(keyEndIndex, numIndex - keyEndIndex), 10);
			if(isNaN(num) || num < 0) {
			    callback('Illegal item, malformed num');
			    return
			}
			if(num == 0) {
			    callback('notfound');
			    return;
			}
			var buf = new Buffer(num + 1);
			fs.read(fd, buf, 0, num + 1, pos + numIndex + 1, function (err, br) {
				var value = buf.toString('utf-8', 0, br - 1);
				// TODO: assert buf[br] = '\n';
				//value = decodeURIComponent(value);
				value = JSON.parse(value);
				callback(null, key, value);
			    });
		    } else {
			callback('Illegal item position, cannot find key');
		    }
		}); 
	}

	if(!opened_fds[filename]) {
	    fs.open(filename, 'r+', function (err, fd) {
		    opened_fds[filename] = fd;
		    readLine(fd);
		});
	} else {
	    readLine(fd);
	}
    }

    instance.delete = function (key, fnok) {
	return instance.set(key, undefined, function () {
		delete cache[key];
		typeof fnok == 'function' && fnok();
	    });
    };

    initialize();
    return instance;
}

exports.opendb = opendb;
