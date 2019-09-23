import { wait } from 'f-promise';
import { devices, Reader, Writer } from 'f-streams';
import { CLOB, Connection, ExecuteOptions, Lob, OUT_FORMAT_OBJECT, Result } from 'oracledb';

/// !doc
/// ## f-streams wrapper for oracle
///
/// `var foracle = require('f-oracle');`
///
let active = 0;
const tracer: any = undefined; // = console.error;

/// * `reader = foracle.reader(connection, sql, [args], [opts])`
export function reader<T>(connection: Connection, sql: string, args?: any[], opts?: ExecuteOptions): Reader<T> {
    args = args || [];
    let rd: Result<T> | undefined, stopped: boolean;
    return devices.generic.reader<T>(
        () => {
            if (!rd && !stopped) {
                tracer && tracer('READER OPEN: ' + ++active + ', SQL:' + sql, args);
                const nopts = {
                    resultSet: true,
                    outFormat: OUT_FORMAT_OBJECT,
                    ...opts,
                };
                rd = wait(connection.execute(sql, args || [], nopts));
            }
            if (!rd || !rd.resultSet) return undefined;
            const row = wait(rd.resultSet.getRow());
            if (!row) {
                wait(rd.resultSet.close());
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
            if (rd && rd.resultSet) wait(rd.resultSet.close());
            tracer && tracer('READER CLOSED: ' + --active);
            rd = undefined;
        },
    );
}

/// * `writer = foracle.writer(connection, sql)`
export function writer<T>(connection: Connection, sql: string): Writer<T> {
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
        wait(connection.execute(sql, values, {}));
    });
}

export function lobReader(lob: Lob) {
    const options = lob.type === CLOB ? { encoding: 'utf8' } : {};
    return devices.node.reader(lob, options);
}

export function lobWriter(lob: Lob) {
    return devices.node.writer(lob);
}
