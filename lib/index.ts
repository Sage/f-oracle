import { wait } from 'f-promise';
import * as ez from 'f-streams';
import { OBJECT, IConnection, IConnectionAttributes, IExecuteReturn, IExecuteOptions, Lob } from 'oracledb';
const generic = ez.devices.generic;

/// !doc
/// ## f-streams wrapper for oracle
/// 
/// `var foracle = require('f-oracle');`
/// 
var active = 0;
var tracer: any; // = console.error;

/// * `reader = foracle.reader(connection, sql, [args], [opts])`  
export function reader(connection: IConnection, sql: string, args?: any[], opts?: IExecuteOptions) {
	args = args || [];
	var rd: IExecuteReturn | undefined, stopped: boolean;
	return generic.reader(() => {
		if (!rd && !stopped) {
			tracer && tracer("READER OPEN: " + ++active + ", SQL:" + sql, args);
			var nopts = Object.assign({
				resultSet: true,
				outFormat: OBJECT,
			}, opts);
			rd = wait(connection.execute(sql, args, nopts) as Promise<IExecuteReturn>);
		}
		if (!rd || !rd.resultSet) return undefined;
		var row = wait(rd.resultSet.getRow() as Promise<Object | any[]>);
		if (!row) {
			wait(rd.resultSet.close() as Promise<void>);
			tracer && tracer("READER CLOSED: " + --active)
			rd = undefined;
			return undefined;
		}
		tracer && tracer("ROW: " + JSON.stringify(row));
		return row;
	}, () => {
		tracer && tracer("READER STOPPED: " + active + ", alreadyStopped=" + stopped);
		stopped = true;
		if (rd && rd.resultSet) wait(rd.resultSet.close() as Promise<void>);
		tracer && tracer("READER CLOSED: " + --active)
		rd = undefined;
	});
}

/// * `writer = foracle.writer(connection, sql)`  
export function writer(connection: IConnection, sql: string) {
	var done: boolean;
	tracer && tracer("writer initialized : " + sql);
	return generic.writer(row => {
		if (row === undefined) { done = true; return; }
		if (done) return;
		var values = Array.isArray(row) ? row : Object.keys(row).map(function (key) { return (row as any)[key] });
		tracer && tracer("Writing values " + JSON.stringify(values));
		wait(connection.execute(sql, values, {}) as Promise<IExecuteReturn>);
	});
}

export function lobReader(lob: Lob) {
	return generic.reader(() => {
		var data = wait<Buffer | string>(cb => lob.iLob.read && lob.iLob.read(cb));
		return data != null ? data : undefined;
	});
}

export function lobWriter(lob: Lob) {
	return generic.writer((data: Buffer | undefined) => {
		if (data !== undefined) wait(cb => lob.iLob.write && lob.iLob.write(data, cb));
		else lob.close();
	});
}
