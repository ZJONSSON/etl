const etl = require('../index');
const pg = require('pg');
const data = require('./data');
const dataChanged = require('./data-changed');
const QueryStream = require('pg-query-stream');
const t = require('tap');

const pool = new pg.Pool({
    host: 'postgres',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'example'
});

const p = etl.postgres.execute(pool);

const before = p.query('CREATE SCHEMA circle_test_schema')
    .catch(Object)
    .then(() => p.query('DROP TABLE IF EXISTS circle_test_schema.test;'))
    .then(() => p.query(
        'CREATE TABLE circle_test_schema.test (' +
        'name varchar(45),' +
        'age integer,' +
        'dt date, ' +
        'CONSTRAINT test_pkey PRIMARY KEY (name)' +
        ')'
    ));

t.test('postgres', async t => {
    await before;
    t.test('collect and script test', async t => {
        const d = await data.stream()
            .pipe(etl.collect(2))
            .pipe(etl.postgres.insert(pool, 'circle_test_schema', 'test', { pushResult: true }))
            .promise();

        t.same(d.length, 1, 'Only one request sent');
        t.same(d[0].rowCount, 3, 'rowCount is correct');
    });

    t.test('and records are verified', async t => {
        const expected = data.data.map(d => ({
            name: d.name,
            age: d.age,
            dt: d.dt
        }))
            .sort((a, b) => a.age - b.age);

        const d = await p.query('SELECT * from circle_test_schema.test order by age');
        t.same(d.rows, expected, 'records verified');
    });

})
.catch(console.log)
.then(() => pool.end());
