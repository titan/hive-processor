"use strict";
const msgpack = require('msgpack-lite');
const nanomsg = require('nanomsg');
const ip = require('ip');
const pg_1 = require('pg');
const redis_1 = require('redis');
class Processor {
    constructor(config) {
        this.addr = config.addr;
        this.functions = new Map();
        this.cachehost = config.cachehost;
        let dbconfig = {
            host: config.dbhost,
            user: config.dbuser,
            database: config.database,
            password: config.dbpasswd,
            port: config.dbport ? config.dbport : 5432,
            min: 1,
            max: 2,
            idleTimeoutMillis: 30000,
        };
        this.pool = new pg_1.Pool(dbconfig);
        this.pool.on('error', function (err, client) {
            console.error('idle client error', err.message, err.stack);
        });
    }
    call(cmd, impl) {
        this.functions.set(cmd, impl);
    }
    run() {
        let pull = nanomsg.socket('pull');
        pull.connect(this.addr);
        let _self = this;
        pull.on('data', (buf) => {
            let pkt = msgpack.decode(buf);
            if (_self.functions.has(pkt.cmd)) {
                _self.pool.connect().then(db => {
                    let cache = redis_1.createClient(6379, _self.cachehost);
                    let func = _self.functions.get(pkt.cmd);
                    func(db, cache, () => {
                        cache.quit();
                        db.release();
                    }, pkt.args);
                }).catch(e => {
                    console.log("DB connection error" + e.stack);
                });
            }
            else {
                console.error(pkt.cmd + " not found!");
            }
        });
    }
}
exports.Processor = Processor;
function rpc(domain, addr, uid, fun, ...args) {
    let p = new Promise(function (resolve, reject) {
        let a = [];
        if (args != null) {
            a = [...args];
        }
        let params = {
            ctx: {
                domain: domain,
                ip: ip.address(),
                uid: uid
            },
            fun: fun,
            args: a
        };
        let req = nanomsg.socket('req');
        req.connect(addr);
        req.on('data', (msg) => {
            resolve(msgpack.decode(msg));
            req.shutdown(addr);
        });
        req.send(msgpack.encode(params));
    });
    return p;
}
exports.rpc = rpc;
function async_serial(ps, acc, scb, fcb) {
    if (ps.length === 0) {
        scb(acc);
    }
    else {
        let p = ps.shift();
        p.then(val => {
            acc.push(val);
            async_serial(ps, acc, scb, fcb);
        }).catch((e) => {
            fcb(e);
        });
    }
}
exports.async_serial = async_serial;
function async_serial_ignore(ps, acc, cb) {
    if (ps.length === 0) {
        cb(acc);
    }
    else {
        let p = ps.shift();
        p.then(val => {
            acc.push(val);
            async_serial_ignore(ps, acc, cb);
        }).catch((e) => {
            async_serial_ignore(ps, acc, cb);
        });
    }
}
exports.async_serial_ignore = async_serial_ignore;
