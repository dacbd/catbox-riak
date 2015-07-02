// Load modules

var Hoek = require('hoek');
var Riak = require('riakpbc');


// Declare internals

var internals = {};


internals.defaults = {
    host: '127.0.0.1',
    port: 8087
};


exports = module.exports = internals.Connection = function (options) {

    Hoek.assert(this.constructor === internals.Connection, 'Riak cache client must be instantiated using new');
    Hoek.assert(typeof options === 'object', 'Must provide configuation to the Riak cache client.');
    Hoek.assert(typeof options.partition === 'string', 'Must specify a partition to use.');
    Hoek.assert(typeof options.ttl_interval === 'number' || options.ttl_interval === false, 'You must provide a ttl_interval or explicitly disable it');

    this.settings = Hoek.applyToDefaults(internals.defaults, options);
    this.client = null;
    return this;
};


internals.Connection.prototype.start = function (callback) {

    if (this.client) {
        return callback();
    }

    this.client = Riak.createClient({
        host: this.settings.host,
        port: this.settings.port
    });
    this.riakgc();
    return callback();
};


internals.Connection.prototype.stop = function () {

    if (this.client) {
        if (this._gcfunc) clearInterval(this._gcfunc);
        this.client.end();
        this.client = null;
    }
};


internals.Connection.prototype.isReady = function () {

    return this.client == null ? false : true;
};


internals.Connection.prototype.validateSegmentName = function (name) {

    if (!name) {
        return new Error('Empty string');
    }

    if (name.indexOf('\0') !== -1) {
        return new Error('Includes null character');
    }

    return null;
};


internals.Connection.prototype.get = function (key, callback) {

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    this.client.get({
        bucket: this.settings.partition,
        key: this.generateKey(key)
    }, function (err, reply) {

        if (err) return callback(err);
        if (!reply.content ||
            !Array.isArray(reply.content)) {

            return callback(null, null);
        }

        var envelope = null;
        try {
            envelope = JSON.parse(reply.content[0].value);
        }
        catch (err) { }

        if (!envelope) {
            return callback(new Error('Bad envelope content'));
        }

        if (!envelope.item ||
            !envelope.stored) {

            return callback(new Error('Incorrect envelope structure'));
        }

        return callback(null, envelope);

    });
};


internals.Connection.prototype.set = function (key, value, ttl, callback) {

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    var envelope = {
        item: value,
        stored: Date.now(),
        ttl: ttl
    };

    var cacheKey = this.generateKey(key);

    var stringifiedEnvelope = null;

    try {
        stringifiedEnvelope = JSON.stringify(envelope);
    }
    catch (err) {
        return callback(err);
    }

    this.client.put({
        bucket: this.settings.partition,
        key: cacheKey,
        content: {
            value: stringifiedEnvelope,
            content_type: 'text/plain',
            indexes: [
                { key: 'ttl_int', value: Date.now() + ttl }
            ]
        }
    }, function (err, reply) {

        return callback(err);
    });
};


internals.Connection.prototype.drop = function (key, callback) {

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    this.client.del({
        key: this.generateKey(key),
        bucket: this.settings.partition
    }, function (err) {

        return callback(err);
    });
};


internals.Connection.prototype.riakgc = function () {

    if (this.settings.ttl_interval === false) {
        this._gcfunc = null;
    } else {
        this._gcfunc = setInterval(function () {

            var stream = this.client.getIndex({
                bucket: this.settings.partition,
                index: 'ttl_int',
                qtype: 1,
                range_min: 1,
                range_max: Date.now()
            });

            stream.on('data', function (reply) {

                return reply.keys.map(function (key) {

                    this.client.del({
                        bucket: this.settings.partition,
                        key: key
                    }, function (err, reply) { });
                }.bind(this));
            }.bind(this));
            stream.on('error', function () { });
        }.bind(this), this.settings.ttl_interval);
    }
    return;
};


internals.Connection.prototype.generateKey = function (key) {

    return encodeURIComponent(key.segment) + ':' + encodeURIComponent(key.id);
};
