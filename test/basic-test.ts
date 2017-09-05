import { assert } from 'chai';
import { IConnection, IConnectionAttributes, getConnection, IPromise } from 'oracledb';
import { wait } from 'f-promise';
import { setup } from 'f-mocha';
import { arrayReader } from 'f-streams';
import { reader, writer } from '..';
setup();

const { ok, deepEqual } = assert;

// deal with oracle's IPromise type
const owait = <T>(p: IPromise<T>) => wait(p as Promise<T>);

describe(module.id, () => {
	var conn: IConnection;

	it('connect', function () {
		const config: IConnectionAttributes = require('../../test/test-config');
		conn = owait(getConnection(config));
		try {
			owait(conn.execute('DROP TABLE T1'));
		} catch (ex) { }
		owait(conn.execute('CREATE TABLE T1 (C1 NUMBER, C2 VARCHAR(10), C3 RAW(8))'));
		ok(true, "connected and table created");
	});

	it('roundtrip', function () {
		const wr = writer<{
			C1: number;
			C2: string;
			C3: Buffer;
		}>(conn, "INSERT INTO T1 (C1, C2, C3) VALUES (:1, :2, :3)");
		const data = [{
			C1: 4,
			C2: "Hello",
			C3: new Buffer("0123456789abcdef", "hex")
		}, {
			C1: 7,
			C2: "World",
			C3: new Buffer("aabbccddeeff0011", "hex")
		},];
		arrayReader(data).pipe(wr);
		const result = reader(conn, "SELECT C1, C2, C3 FROM T1").toArray();
		deepEqual(result, data);
	});
});