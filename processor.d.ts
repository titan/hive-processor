import * as Pool from 'pg-pool';
import { Client as PGClient } from 'pg';
import { RedisClient } from 'redis';
export interface Config {
    dbhost: string;
    dbuser: string;
    dbport?: number;
    database: string;
    dbpasswd: string;
    cachehost: string;
    addr: string;
}
export interface DoneFunction {
    (): void;
}
export interface ModuleFunction {
    (db: PGClient, cache: RedisClient, done: DoneFunction, ...args: any[]): void;
}
export declare class Processor {
    functions: Map<string, ModuleFunction>;
    addr: string;
    pool: Pool;
    cachehost: string;
    constructor(config: Config);
    call(cmd: string, impl: ModuleFunction): void;
    run(): void;
}
export declare function rpc(domain: string, addr: string, uid: string, fun: string, ...args: any[]): Promise<any>;
