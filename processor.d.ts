import { Pool, Client as PGClient } from 'pg';
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
export declare function rpc<T>(domain: string, addr: string, uid: string, fun: string, ...args: any[]): Promise<T>;
export declare function async_serial<T>(ps: Promise<T>[], acc: T[], scb: (vals: T[]) => void, fcb: (e: Error) => void): void;
export declare function async_serial_ignore<T>(ps: Promise<T>[], acc: T[], cb: (vals: T[]) => void): void;
