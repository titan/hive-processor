"use strict";
const msgpack = require('msgpack-lite');
const nanomsg = require('nanomsg');
const Pool = require('pg-pool');
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
            port: 5432,
            min: 1,
            max: 2,
            idleTimeoutMillis: 30000,
        };
        this.pool = new Pool(dbconfig);
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
                let func = _self.functions.get(pkt.cmd);
                _self.pool.connect().then(db => {
                    let cache = redis_1.createClient(6379, _self.cachehost);
                    func(db, cache, () => {
                        cache.quit();
                        db.end();
                    }, pkt.args);
                });
            }
            else {
                console.error(pkt.cmd + " not found!");
            }
        });
    }
}
exports.Processor = Processor;