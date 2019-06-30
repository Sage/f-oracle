import { wait } from 'f-promise';
import { devices, Reader, Writer } from 'f-streams';
import { IConnection, IExecuteOptions, IExecuteReturn, Lob, OBJECT } from 'oracledb';

/// !doc
/// ## f-streams wrapper for oracle
///
/// `var foracle = require('f-oracle');`
///
let active = 0;
const tracer: any = undefined; // = console.error;

/// * `reader = foracle.reader(connection, sql, [args], [opts])`
export function reader<T>(connection: IConnection, sql: string, args?: any[], opts?: IExecuteOptions): Reader<T> {
    args = args || [];
    let rd: IExecuteReturn | undefined, stopped: boolean;
    return devices.generic.reader<T>(
        () => {
            if (!rd && !stopped) {
                tracer && tracer('READER OPEN: ' + ++active + ', SQL:' + sql, args);
                const nopts = {
                    resultSet: true,
                    outFormat: OBJECT,
                    ...opts,
                };
                rd = wait(connection.execute(sql, args, nopts) as Promise<IExecuteReturn | undefined>);
            }
            if (!rd || !rd.resultSet) return undefined;
            const row = wait(rd.resultSet.getRow() as Promise<T | undefined>);
            if (!row) {
                wait(rd.resultSet.close() as Promise<void>);
                tracer && tracer('READER CLOSED: ' + --active);
                rd = undefined;
                return undefined;
            }
            tracer && tracer('ROW: ' + JSON.stringify(row));
            return row;
        },
        () => {
            tracer && tracer('READER STOPPED: ' + active + ', alreadyStopped=' + stopped);
            stopped = true;
            if (rd && rd.resultSet) wait(rd.resultSet.close() as Promise<void>);
            tracer && tracer('READER CLOSED: ' + --active);
            rd = undefined;
        },
    );
}

/// * `writer = foracle.writer(connection, sql)`
export function writer<T>(connection: IConnection, sql: string): Writer<T> {
    let done: boolean;
    tracer && tracer('writer initialized : ' + sql);
    return devices.generic.writer<T>(row => {
        if (row === undefined) {
            done = true;
            return;
        }
        if (done) return;
        const values = Array.isArray(row)
            ? row
            : Object.keys(row).map(function(key) {
                  return (row as any)[key];
              });
        tracer && tracer('Writing values ' + JSON.stringify(values));
        wait(connection.execute(sql, values, {}) as Promise<IExecuteReturn>);
    });
}

export function lobReader(lob: Lob) {
    return devices.generic.reader(() => {
        const data = wait<Buffer | string>(cb => lob.iLob.read && lob.iLob.read(cb));
        return data != null ? data : undefined;
    });
}

export function lobWriter(lob: Lob) {
    return devices.generic.writer((data: Buffer | undefined) => {
        if (data !== undefined) wait(cb => lob.iLob.write && lob.iLob.write(data, cb));
        else lob.close();
    });
}
