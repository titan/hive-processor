import * as msgpack from 'msgpack-lite';
import * as nanomsg from 'nanomsg';
import * as Pool from 'pg-pool';
import * as ip from 'ip';
import { Client as PGClient } from 'pg';
import { createClient, RedisClient} from 'redis';

export interface Config {
  dbhost: string,
  dbuser: string,
  dbport?: number,
  database: string,
  dbpasswd: string,
  cachehost: string, 
  addr: string
}

export interface DoneFunction {
  (): void;
}

export interface ModuleFunction {
  (db: PGClient, cache: RedisClient, done: DoneFunction, ...args: any[]): void;
}

export class Processor {
  functions: Map<string, ModuleFunction>;
  addr: string;
  pool: Pool;
  cachehost: string;

  constructor(config: Config) {
    this.addr = config.addr;
    this.functions = new Map<string, ModuleFunction>();
    this.cachehost = config.cachehost;
    let dbconfig = {
      host: config.dbhost,
      user: config.dbuser,
      database: config.database,
      password: config.dbpasswd,
      port: config.dbport? config.dbport: 5432,
      min: 1, // min number of clients in the pool
      max: 2, // max number of clients in the pool
      idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
    };
    this.pool = new Pool(dbconfig);
    this.pool.on('error', function (err, client) {
      console.error('idle client error', err.message, err.stack)
    })
  }

  public call(cmd: string, impl: ModuleFunction): void {
    this.functions.set(cmd, impl);
  }

  public run(): void {
    let pull = nanomsg.socket('pull');
    pull.connect(this.addr);
    let _self = this;
    pull.on('data', (buf: NodeBuffer) => {
      let pkt = msgpack.decode(buf);
      if (_self.functions.has(pkt.cmd)) {
        _self.pool.connect().then(db => {
          let cache = createClient(6379, _self.cachehost);
          let func = _self.functions.get(pkt.cmd);
          func(db, cache, () => {
            cache.quit();
            db.end();
          }, pkt.args);
        });
      } else {
        console.error(pkt.cmd + " not found!");
      }
    });
  }
}

export function rpc(domain: string, addr: string, uid: string, fun: string, ...args: any[]): Promise<any> {
  let p = new Promise(function (resolve, reject) {
    let a = [];
    if (args != null) {
      a = [...args];
    }
    let params = {
      ctx: {
        domain: domain,
        ip:     ip.address(),
        uid:    uid
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
