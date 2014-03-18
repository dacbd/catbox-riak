// Load modules

var Lab = require('lab');
var Catbox = require('catbox');
var Riak = require('..');


// Declare internals

var internals = {
    defaults: {
        ttl_interval: false
    }
};


// Test shortcuts

var expect = Lab.expect;
var before = Lab.before;
var after = Lab.after;
var describe = Lab.experiment;
var it = Lab.test;

describe('Riak', function () {

    it('throws an error if not created with new', function (done) {

        var fn = function () {

            var riak = Riak();
        };

        expect(fn).to.throw(Error);
        done();
    });

    it('creates a new connection', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            expect(client.isReady()).to.equal(true);
            done();
        });
    });

    it('closes the connection', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            expect(client.isReady()).to.equal(true);
            client.stop();
            expect(client.isReady()).to.equal(false);
            done();
        });
    });

    it('gets an item after settig it', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, '123', 500, function (err) {

                expect(err).to.not.exist;
                client.get(key, function (err, result) {

                    expect(err).to.equal(null);
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });

    it('fails setting an item circular references', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            var value = { a: 1 };
            value.b = value;
            client.set(key, value, 10, function (err) {

                expect(err.message).to.equal('Converting circular structure to JSON');
                done();
            });
        });
    });

    it('fails setting an item with very long ttl', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, '123', Math.pow(2, 31), function (err) {

                expect(err.message).to.equal('Invalid ttl (greater than 2147483647)');
                done();
            });
        });
    });

    it('ignored starting a connection twice on same event', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        var x = 2;
        var start = function () {

            client.start(function (err) {

                expect(client.isReady()).to.equal(true);
                --x;
                if (!x) {
                    done();
                }
            });
        };

        start();
        start();
    });

    it('ignored starting a connection twice chained', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            expect(err).to.not.exist;
            expect(client.isReady()).to.equal(true);

            client.start(function (err) {

                expect(err).to.not.exist;
                expect(client.isReady()).to.equal(true);
                done();
            });
        });
    });

    it('returns not found on get when using null key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.get(null, function (err, result) {

                expect(err).to.equal(null);
                expect(result).to.equal(null);
                done();
            });
        });
    });

    it('returns not found on get when item expired', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, 'x', 1, function (err) {

                expect(err).to.not.exist;
                setTimeout(function () {

                    client.get(key, function (err, result) {

                        expect(err).to.equal(null);
                        expect(result).to.equal(null);
                        done();
                    });
                }, 2);
            });
        });
    });

    it('returns error on set when using null key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.set(null, {}, 1000, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when using invalid key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.get({}, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on drop when using invalid key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.drop({}, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on set when using invalid key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.set({}, {}, 1000, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('ignores set when using non-positive ttl value', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, 'y', 0, function (err) {

                expect(err).to.not.exist;
                done();
            });
        });
    });

    it('returns error on drop when using null key', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.start(function (err) {

            client.drop(null, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when stopped', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.get(key, function (err, result) {

            expect(err).to.exist;
            expect(result).to.not.exist;
            done();
        });
    });

    it('returns error on set when stopped', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.set(key, 'y', 1, function (err) {

            expect(err).to.exist;
            done();
        });
    });

    it('returns error on drop when stopped', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.drop(key, function (err) {

            expect(err).to.exist;
            done();
        });
    });

    it('returns error on missing segment name', function (done) {

        var config = {
            expiresIn: 50000
        };
        var fn = function () {

            var client = new Catbox.Client(Riak, internals.defaults);
            var cache = new Catbox.Policy(config, client, '');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error on bad segment name', function (done) {

        var config = {
            expiresIn: 50000
        };
        var fn = function () {

            var client = new Catbox.Client(Riak, internals.defaults);
            var cache = new Catbox.Policy(config, client, 'a\0b');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error when cache item dropped while stopped', function (done) {

        var client = new Catbox.Client(Riak, internals.defaults);
        client.stop();
        client.drop('a', function (err) {

            expect(err).to.exist;
            done();
        });
    });

    describe('#start', function () {

        it('sets client to when the connection succeeds', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            riak.start(function (err, result) {

                expect(err).to.not.exist;
                expect(result).to.not.exist;
                expect(riak.client).to.exist;
                done();
            });
        });

        it('reuses the client when a connection is already started', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            riak.start(function (err) {

                expect(err).to.not.exist;
                var client = riak.client;

                riak.start(function () {

                    expect(client).to.equal(riak.client);
                    done();
                });
            });
        });
    });

    describe('#validateSegmentName', function () {

        it('returns an error when the name is empty', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            var result = riak.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });

        it('returns an error when the name has a null character', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            var result = riak.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('returns null when there aren\'t any errors', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            var result = riak.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });

    describe('#get', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);

            riak.get('test', function (err) {

                expect(err).to.exist;
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('passes an error to the callback when there is an error returned from getting an item', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.client = {
                get: function (item, callback) {

                    callback(new Error());
                }
            };

            riak.get('test', function (err) {

                expect(err).to.exist;
                expect(err).to.be.instanceOf(Error);
                done();
            });
        });

        it('passes an error to the callback when there is an error parsing the result', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.client = {
                get: function (item, callback) {

                    callback(null, { content: [{ value: 'test' }] });
                }
            };

            riak.get('test', function (err) {

                expect(err).to.exist;
                expect(err.message).to.equal('Bad envelope content');
                done();
            });
        });

        it('passes an error to the callback when there is an error with the envelope structure', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.client = {
                get: function (item, callback) {

                    callback(null, { content: [{ value: '{ "item": "false" }' }] });
                }
            };

            riak.get('test', function (err) {

                expect(err).to.exist;
                expect(err.message).to.equal('Incorrect envelope structure');
                done();
            });
        });

        it('is able to retrieve an object thats stored when connection is started', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'wwwtest',
                ttl_interval: false
            };
            var key = {
                id: 'test',
                segment: 'test'
            };

            var riak = new Riak(options);

            riak.start(function () {

                riak.set(key, 'myvalue', 200, function (err) {

                    expect(err).to.not.exist;
                    riak.get(key, function (err, result) {

                        expect(err).to.not.exist;
                        expect(result.item).to.equal('myvalue');
                        done();
                    });
                });
            });
        });

        it('returns null when unable to find the item', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'wwwtest',
                ttl_interval: false
            };
            var key = {
                id: 'notfound',
                segment: 'notfound'
            };

            var riak = new Riak(options);

            riak.start(function () {

                riak.get(key, function (err, result) {

                    expect(err).to.not.exist;
                    expect(result).to.not.exist;
                    done();
                });
            });
        });
    });

    describe('#set', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'errortest',
                ttl_interval: false
            };

            var riak = new Riak(options);

            riak.set('test1', 'test1', 3600, function (err) {

                expect(err).to.exist;
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('test for proper ttl bubbling', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'ttltest',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.start(function () {

                riak.set('test1', 'test1', 3600, function (err) {

                    expect(err).to.not.exist;

                    riak.set('test2', 'test2', 300, function (err) {

                        expect(err).to.not.exist;
                        done();
                    });
                });
            });
        });

        it('passes an error to the callback when there is an error returned from setting an item', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'errortest',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.client = {
                put: function (key, callback) {

                    callback(new Error());
                },
                getIndex: function () {
                    var fakestream = new require('stream').Readable({ objectMode: true });
                    fakestream._read = function () {
                        this.push({ keys: ['a'] });
                        this.push(null);
                    };
                    return fakestream;
                },
                del: function (q, cb) {
                    cb();
                }
            };

            riak.set('test', 'test', 3600, function (err) {

                expect(err).to.exist;
                expect(err).to.be.instanceOf(Error);
                done();
            });
        });

        it('deletes an expired key in a timely manner', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'expiretest',
                ttl_interval: 300
            };
            var riak = new Riak(options);
            riak.start(function () {

                riak.client = {
                    put: function (key, callback) {

                        callback();
                    },
                    getIndex: function () {

                        var fakestream = new require('stream').Readable({ objectMode: true });
                        fakestream._read = function () {
                            this.push({ keys: ['a'] });
                            this.push(null);
                        };
                        return fakestream;
                    },
                    del: function (q, cb) {

                        cb();
                        done();
                    }
                };
                riak.set('test', 'test', 200, function (err) {

                    expect(err).to.not.exist;
                });
            });
        });
    });

    describe('#drop', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'errortest',
                ttl_interval: false
            };

            var riak = new Riak(options);

            riak.drop('test2', function (err) {

                expect(err).to.exist;
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('deletes the item from riak', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 8087,
                partition: 'test',
                ttl_interval: false
            };

            var riak = new Riak(options);
            riak.client = {
                del: function (key, callback) {

                    callback(null, null);
                }
            };

            riak.drop('test', function (err) {

                expect(err).to.not.exist;
                done();
            });
        });
    });
});

describe('#stop', function () {

    it('sets the client to null', function (done) {

        var options = {
            host: '127.0.0.1',
            port: 8087,
            partition: 'test',
            ttl_interval: false
        };

        var riak = new Riak(options);

        riak.start(function () {

            expect(riak.client).to.exist;
            riak.stop();
            expect(riak.client).to.not.exist;
            done();
        });
    });
});

