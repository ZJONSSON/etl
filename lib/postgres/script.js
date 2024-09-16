const Postgres = require('./postgres');

const quoteString = (str) => `"${str}"`;

class Script extends Postgres {
  constructor(pool, schema, table, options) {
    super(pool, options);

    this.schema = schema;
    this.table = table;
    this.columns = this.getColumns();
    this.upsert = options.upsert;
    if (this.upsert) this.pkeys = this.getPrimaryKeys();
    this.prefix = this.options.prefix || 'INSERT INTO';
    this.maxBuffer = options.maxBuffer || 1024 * 1024;
  }

  getColumns() {
    const sql = 'SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE ' +
      `TABLE_SCHEMA='${this.schema}' AND TABLE_NAME='${this.table}';`;

    return this.query(sql)
      .then(d => {
        d = d.rows;
        if (!d.length)
          throw 'TABLE_NOT_FOUND';
        return d.map(d => d.column_name);
      });
  }

  getPrimaryKeys() {
    const sql = 'SELECT a.attname' +
      ' FROM   pg_index i' +
      ' JOIN   pg_attribute a ON a.attrelid = i.indrelid' +
      ' AND a.attnum = ANY(i.indkey)' +
      ` WHERE  i.indrelid = '${this.schema}.${this.table}'::regclass` +
      ' AND    i.indisprimary;';

    return this.query(sql)
      .then(d => d.rows.map(d => d.attname))
      .catch(e => {
        if (e.message && e.message.includes('op ANY/ALL'))
          return [];
        else
          throw e;
      });
  }

  _push() {
    if (this.buffer) {
      this.push(this.buffer);
    }
    this.buffer = undefined;
  }

  _fn(record) {
    return Promise.all([this.columns, this.pkeys]).then(data => {
      const columns = data[0];
      const pkeys = data[1] || [];
      const darr = (Array.isArray(record)) ? record : new Array(record);

      for (var i = 0; i < darr.length; i++) {
        var d = darr[i];
        
        if (!this.buffer)
            this.buffer = `${this.prefix} ${this.schema}.${this.table} ( ${columns.map(quoteString).join(',')} ) VALUES `;
        else
            this.buffer += ', ';
        
        this.buffer += '(' + columns.map(key => {
            const value = d[key];
            if (typeof value === 'undefined')
                return 'DEFAULT';
            else if (value === null)
                return 'null';
            else if (typeof value === 'object')
                return escapeLiteral(JSON.stringify(value));
            else
                return escapeLiteral(value);
        })
            .join(',') + ')';
      }

      if (this.upsert) {
        let tmp_arr = [];
        for (let i = 0, l = columns.length; i < l; i++) {
          const value = d[columns[i]];
          if (typeof value === 'undefined')
            continue;

          let sql = `"${columns[i]}" =`;
          if (value === null)
            sql += 'null';
          else if (typeof value === 'object')
            sql += escapeLiteral(JSON.stringify(value));
          else
            sql += escapeLiteral(value);

          tmp_arr.push(sql);
        }
        if (tmp_arr.length && pkeys.length) {
          this.buffer += ` ON CONFLICT (${pkeys.map(quoteString).join(', ')}) DO UPDATE SET ${tmp_arr.join(', ')}`;
        }
        this._push();
      }
      if (this.buffer && this.buffer.length > this.maxBuffer) this._push();
    });
  }

  _flush(cb) {
    this._push();
    setImmediate(() => Postgres.prototype._flush(cb));
  }
}

// https://github.com/brianc/node-postgres/blob/83a946f61cb9e74c7f499e44d03a268c399bd623/lib/client.js
function escapeLiteral(str) {
  let hasBackslash = false;
  let escaped = '\'';

  if (typeof str !== 'string')
    return str;

  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }

  escaped += '\'';

  if (hasBackslash === true)
    escaped = ' E' + escaped;

  return escaped;
}

module.exports = (...params) => new Script(...params);
