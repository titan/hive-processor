"use strict";
const msgpack = require("msgpack-lite");
const crypto = require("crypto");
const nanomsg = require("nanomsg");
const ip = require("ip");
const bluebird = require("bluebird");
const zlib = require("zlib");
const pg_1 = require("pg");
const redis_1 = require("redis");
class Processor {
    constructor(config) {
        this.addr = config.addr;
        this.functions = new Map();
        this.cachehost = config.cachehost;
        this.cacheport = config.cacheport ? config.cacheport : (process.env["CACHE_PORT"] ? parseInt(process.env["CACHE_PORT"]) : 6379);
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
                    let cache = bluebird.promisifyAll(redis_1.createClient(this.cacheport, _self.cachehost, { "return_buffers": true }));
                    let func = _self.functions.get(pkt.cmd);
                    if (pkt.args) {
                        func(db, cache, () => {
                            cache.quit();
                            db.release();
                        }, ...pkt.args);
                    }
                    else {
                        func(db, cache, () => {
                            cache.quit();
                            db.release();
                        });
                    }
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
        const sn = crypto.randomBytes(64).toString("base64");
        let req = nanomsg.socket('req');
        req.connect(addr);
        req.on('data', (msg) => {
            const data = msgpack.decode(msg);
            if (sn === data["sn"]) {
                if (data["payload"][0] === 0x78 && data["payload"][1] === 0x9c) {
                    zlib.inflate(data["payload"], (e, newbuf) => {
                        if (e) {
                            reject(e);
                        }
                        else {
                            resolve(msgpack.decode(newbuf));
                        }
                    });
                }
                else {
                    resolve(msgpack.decode(data["payload"]));
                }
            }
            else {
                reject(new Error("Invalid calling sequence number"));
            }
            req.shutdown(addr);
        });
        req.send(msgpack.encode({ sn, pkt: params }));
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
