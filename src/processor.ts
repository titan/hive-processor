import * as msgpack from 'msgpack-lite';
import * as nanomsg from 'nanomsg';
import * as Pool from 'pg-pool';
import { Client as PGClient } from 'pg';
import { createClient, RedisClient} from 'redis';

export interface Config {
  dbhost: string,
  dbuser: string,
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
      port: 5432,
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
        let func = _self.functions.get(pkt.cmd);
        _self.pool.connect().then(db => {
          let cache = createClient(6379, _self.cachehost);
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
