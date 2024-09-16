[![NPM Version][npm-image]][npm-url]
[![NPM Downloads][downloads-image]][downloads-url]
[![code coverage](https://zjonsson.github.io/node-etl/badge.svg)](https://zjonsson.github.io/node-etl/) 


[npm-image]: https://img.shields.io/npm/v/etl.svg
[npm-url]: https://npmjs.org/package/etl
[downloads-image]: https://img.shields.io/npm/dm/etl.svg
[downloads-url]: https://npmjs.org/package/etl

ETL is a collection of stream based components that can be piped together to form a complete ETL pipeline with buffering, bulk-inserts and concurrent database streams. See the `test` directory for live examples.

```
npm install etl
```

Introductory example: csv -> elasticsearch 

```js
fs.createReadStream('scores.csv')
  // parse the csv file
  .pipe(etl.csv())
  // map `date` into a javascript date and set unique _id
  .pipe(etl.map(d => {
    d._id = d.person_id;
    d.date = new Date(d.date);
    return d;
  }))
  // collect 1000 records at a time for bulk-insert
  .pipe(etl.collect(1000))  
  // upsert records to elastic with max 10 concurrent server requests
  .pipe(etl.elastic.index(esClient,'scores','records',{concurrency:10}))
  // Switch from stream to promise chain and report done or error
  .promise()
  .then( () => console.log('done'), e => console.log('error',e));
```

## API Reference
* [Parsers](#parsers)
* [Transforms](#transforms)
* [Database upload](#databases)
  * [Mongodb](#mongo)
  * [Mysql](#mysql)
  * [Postgres](#postgres)
  * [Elasticsearch](#elasticsearch)
  * [BigQuery](#bigquery)
* [Utilities](#utilities)

### Parsers 

<a name="csv" href="#csv">#</a> etl.<b>csv</b>([<i>options</i>])

Parses incoming csv text into individual records.  For parsing options see [csv-parser](https://www.npmjs.com/package/csv-parser).  If  `options` contains a `transform` object containing functions, those functions will be applied on the values of any matching keys in the data.  If a key in the `transform` object is set to `null` then value with that key will not be included in the downstream packets.  If option `santitize` is set to true, then headers will be trimmed, converted to lowercase, spaces converted to underscore and any blank values (empty strings) will be set to undefined. The option `addLineNumbers` is `true` by default, if is set to `false` (boolean) the result won't have the `__line` numbers for each object.

A `header` event will be emitted when headers have been parsed. An event listener can change the headers in-place before the stream starts piping out parsed data.

Example

```js
// Here the test.csv is parsed but field dt is converted to date.  Each packet will
// contain the following properties:  __filename, __path, __line and csv fields
etl.file('test.csv')
  .pipe(etl.csv({
    transform: {
      dt : function(d) {
        return new Date(d);
      }
    }
  })
  .pipe(...)
```

<a name="fixed" href="#fixed">#</a> etl.<b>fixed</b>(<i>layout</i>)

Parses incoming text into objects using a fixed width layout.   The layout should be an object where each key is a field name that should be parsed, containing an object with `start`, `end` and/or `length`.  Alternatively each key can just have a number, which will be deemed to be `length`. 

If a key contains a `transform` function, it will be applied to the parsed value of that key. 


The layout can also be supplied as an array where instead of an object key the fieldname is defined using property `field` in each element.

The length of a single record will be determined by the highest `end` or `start+length` position.

Each packet will contain `__line`, a number signifying the sequential position of the record

Example

```js
// Reads the file and parses into individual records.  Each packet will contain the
// following properties:  __filename, __path, __line, firstCol, nextCol, lastCol.
// nextCol values are coerced into numbers here

var layout = {
  firstCol: {start: 0, end:10},
  nextCol: {length:5, transform: Number},
  lastCol: 5
}

etl.file('test.txt')
  .pipe(etl.fixed(layout))
  .pipe(...)
```

If needed, you can get the current line as param in the transform function.

```js

var layout = {
  firstCol: {
    start: 0, end: 10,
    transform: function (value, line) {      
      console.log(`firstCol on line ${line} is: ${value}`); // firstCol on line 1 is: test
      return value;
    }
  }
}

```

### Transforms

<a name="map" href="#map">#</a> etl.<b>map</b>(<i>fn</i>)

The base [`streamz`](http://github.com/ZJONSSON/streamz) object is exposed as `etl.map` to provide quick ability to do mappings on the fly.  Anything `pushed` inside the custom function will go downstream.  Also if the function has a return value (or a promise with a return value) that is `!== undefined` that return value will be pushed as well.

Example

```js
// In this example names and ages and normalized to fresh objects pushed
// downstream.  (If we wanted to retain metadata we would use Object.create(d))

etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.map(function(d) {
    this.push({name: d.name_1, age: d.age_1});
    this.push({name: d.name_2, age: d.age_2});
    this.push({name: d.name_3, age: d.age_3});
  }))
```

<a name="split" href="#split">#</a> etl.<b>split</b>([<i>symbol</i>])

Splits the `text` of an incoming stream by the provided symbol into separate packets. The default symbol is a newline, i.e. splitting incoming text into invididual lines.  If the supplied symbol is never found, the incoming stream will be buffered until the end, where all content is sent in one chunk.  The prototype of each packet is the first incoming packet for each buffer.

Example

```js
// Reads the file and splits `text` by newline
etl.file('text.txt')
  .pipe(etl.split())
```

<a name="cut" href="#cut">#</a> etl.<b>cut</b>(<i>maxLength</i>)

Cuts incoming text into text snippets of a given maximum length and pushes downstream.

<a name="collect" href="#collect">#</a> etl.<b>collect</b>(<i>count</i> [,<i>maxDuration</i>] [,<i>maxTextLength</i>])

Buffers incoming packets until they reach a specified count and then sends the array of the buffered packets downstream. At the end of the incoming stream, any buffered items are shipped off even though the count has not been achieved.   This functionality can come in handy when preparing to bulk-insert into databases.   An optional `maxDuration` can be supplied to signal the maximum amount of time (in ms) that can pass from a new collection starting until the contents are pushed out.  This can come in handy when processing real time sporadic data where we want the collection to flush early even if the count has not been reached.  Finally defining an optional `maxTextLength` will cause the stream to keep track of the stringified length of the puffer and push when it goes over the limit. 

Example:

```js
var data = [1,2,3,4,5,6,7,8,9,10,11],
    collect = etl.collect(3);

data.forEach(collect.write.bind(collect));
collect.end();

collect.pipe(etl.inspect());
// Should show 4 packets: [1,2,3]   [4,5,6]    [7,8,9]   [10,11]
```

If the first argument (`count`) is a function it will be used as a custom collection function.  This function can add elements to the buffer by: `this.buffer.push(data)` and push buffer downstream by: `this._push()`.   When stream is ended any remaining buffer is pushed automatically.

<a name="chain" href="#chain">#</a> etl.<b>chain</b>(<i>fn</i>)

Allows a custom subchain of streams to be injected into the pipe using duplexer3. You must provide a custom function that takes in the inbound stream as a first argument and optionally an outbound stream as the second argument.   You can use the optional outbound stream directly to chain the two streams together or you can return a stream or a Promise resolved with stream or values (all of which will be piped down with `etl.toStream`).

Example 1: Simple return of the outbound stream

```js
etl.file('test.csv')
  .pipe(etl.chain(function(inbound) {
    return inbound
      .pipe(etl.csv())
      .pipe(etl.collect(100));
  }))
  .pipe(console.log);
```

Example: Using the outbond stream from arguments
```js
etl.file('test.csv')
  .pipe(etl.chain(function(inbound,outbound) {
    inbound
      .pipe(etl.csv())
      .pipe(etl.collect(100))
      .pipe(outbound);
  }))
  .pipe(console.log);
```

<a name="prescan" href="#prescan">#</a> etl.<b>prescan</b>(<i>size</i>,<i>fn</i>)

Buffers the incoming data until the supplied size is reached (either number of records for objects or buffer/string length). When target size is reached, the supplied function will be called with the buffered data (array) as an argument. After the function has executed and the returning promise (if any) has been resolved, all buffered data will be piped downstream as well as all subsequent data.

Prescan allows the user to make certain determinations from the incoming data before passing it down, such as inspecting data types across multiple rows.


Example:

```js
// In this example we want to collect all columns for first 10 rows
// of a 
// to build a csv header row

let headers = new Set();
fs.createReadStream('data.json')
  .pipe(etl.split()) // split on newline
  .pipe(etl.map(d => JSON.parse(d)))  // parse each line as json
  .pipe(etl.prescan(10,d => 
    // build up headers from the first 10 lines
    d.forEach(d => Object.keys(d).forEach(key => headers.add(key)))
  ))
  .pipe(etl.map(function(d) => {
    this.firstline = this.firstline || this.push([...headers].join('.')+'\n');
    headers.map(header => d[header]).join('.')+'\n'))
  }))
  .pipe(fs.createWriteStream('data.csv'))
```

<a name="expand" href="#expand">#</a> etl.<b>expand</b>([<i>convert</i>])

Throughout the etl pipeline new packets are generated with incoming packets as prototypes (using `Object.create`).  This means that inherited values are not enumerable and will not show up in stringification by default (although they are available directly).  `etl.expand()` loops through all keys of an incoming packet and explicitly sets any inherited values as regular properties of the object

Example:

```js
// In this example the `obj` would only show property `c` in stringify
// unless expanded first
var base = {a:1,b:'test'},
    obj = Object.create(base),
    s = etl.streamz();

obj.c = 'visible';
s.end(obj);

s.pipe(etl.expand())
  .pipe(etl.inspect());
```

The optional `convert` option will modify the keys of the new object.  If `convert` is `'uppercase'` or `'lowercase'` the case of the keys will be adjusted accordingly.  If `convert` is a function it will set the keyname to the function output (and if output is `undefined` that particular key will not be included in the new object)

<a name="stringify" href="#stringify">#</a> etl.<b>stringify</b>([<i>indent</i>] [,<i>replacer</i>] [,<i>newline</i>])

Transforms incoming packets into JSON stringified versions, with optional `indent` and `replacer`.  If `newline` is true a `\n` will be appended to each packet.

<a name="inspect" href="#inspect">#</a> etl.<b>inspect</b>([<i>options</i>])

Logs incoming packets to console using `util.inspect` (with optional custom options)

<a name="timeout" href="#timeout">#</a> etl.<b>timeout</b>([<i>ms</i>])

A passthrough transform that emits an error if no data has passed through for at least the supplied milliseconds (`ms`).  This is useful to manage pipelines that go stale for some reason and need to be errored out for further inspection.

Example:

```js
// Here the pipeline times out if no data has been flowing to the file for at least 1 second
mongocollection.find({})
  .pipe(lookup)
  .timeout(1000)
  .pipe(etl.toFile('test.json'))
```


### Databases

#### Mongo

<a name="mongoinsert" href="#mongoinsert">#</a> etl.mongo.<b>insert</b>(<i>collection</i> [,<i>options</i>])

Inserts incoming data into the provided mongodb collection.  The supplied collection can be a promise on a collection.  The options are passed on to both streamz and the mongodb insert comand.  By default this object doesn't push anything downstream, but if `pushResults` is set as `true` in options, the results from mongo will be pushed downstream.

Example

```js
// The following inserts data from a csv, 10 records at a time into a mongo collection
// ..assuming mongo has been promisified

var db = mongo.ConnectAsync('mongodb://localhost:27017/testdb');
var collection = db.then(function(db) {
  return db.collection('testcollection');
});

etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.collect(10))
  .pipe(etl.mongo.insert(collection));

```

<a name="mongoupdate" href="#mongoupdate">#</a> etl.mongo.<b>update</b>(<i>collection</i> [,<i>keys</i>] [,<i>options</i>])

Updates incoming data by building a `criteria` from an array of `keys` and the incoming data.   Supplied collection can be a promise and results can be pushed downstream by declaring `pushResults : true`.   The options are passed to mongo so defining `upsert : true` in options will ensure an upsert of the data.

Example

```js
// The following updates incoming persons using the personId as a criteria (100 records at a time)

etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.collect(100))
  .pipe(etl.mongo.update(collection,['personId']));

```

<a name="mongouupsert" href="#mongoupsert">#</a> etl.mongo.<b>upsert</b>(<i>collection</i> [,<i>keys</i>] [,<i>options</i>])

Syntax sugar for `mongo.update` with `{upsert: true}`

By default `update` and `upsert` will take each data object and wrap it within a `$set{}`.  If you want to have full control of the mongo update verbs used you can put them under `$update` in the data object.

#### Mysql

<a name="mysqlupsert" href="#mysqlupsert">#</a> etl.mysql.<b>upsert</b>(<i>pool</i>, <i>schema</i>, <i>table</i> [,<i>options</i>])

Pipeline that scripts incoming packets into bulk sql commands (`etl.mysql.script`) and executes them (`etl.mysql.execute`) using the supplied mysql pool. When the size of each SQL command reaches `maxBuffer` (1mb by default) the command is sent to the server.  Concurrency is managed automatically by the mysql poolSize. 

Example:

```js
etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.mysql.upsert(pool,'testschema','testtable',{concurrency:4 }))
```

<a name="mysqlscript" href="#mysqlscript">#</a> etl.mysql.<b>script</b>(<i>pool</i>, <i>schema</i>, <i>table</i> [,<i>options</i>])

Collects data and builds up a mysql statement to insert/update data until the buffer is more than `maxBuffer` (customizable in options).  Then the maxBuffer is reached, a full sql statement is pushed downstream.   When the input stream has ended, any remaining sql statement buffer will be flushed as well. 

The script stream first establishes the column names of the table being updated, and as data comes in - it uses only the properties that match column names in the table.

<a name="mysqlexecute" href="#mysqlexecute">#</a> etl.mysql.<b>execute</b>(<i>pool</i> [,<i>options</i>])

This component executes any incoming packets as sql statements using connections from the connection pool. The maximum concurrency is automatically determined by the mysql poolSize, using the combination of callbacks and Promises.

Example:

```js
// The following bulks data from the csv into sql statements and executes them with 
// a maximum of 4 concurrent connections

etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.mysql.script(pool,'testschema','testtable'))
  .pipe(etl.mysql.execute(pool,4))
```

#### Postgres

<a name="postgresupsert" href="#postgresupsert">#</a> etl.postgres.<b>upsert</b>(<i>pool</i>, <i>schema</i>, <i>table</i> [,<i>options</i>])

Pipeline that scripts incoming packets into bulk sql commands (`etl.postgres.script`) and executes them (`etl.postgres.execute`) using the supplied postgres pool. When the size of each SQL command reaches `maxBuffer` (1mb by default) the command is sent to the server.  Concurrency is managed automatically by the postgres poolSize. If primary key is defined and an incoming data packet contains a primary key that already exists in the table, the record will be updated - otherwise the packet will be inserted.


Example:

```js
etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.postgres.upsert(pool,'testschema','testtable',{concurrency:4 }))
```


<a name="postgresscript" href="#postgresscript">#</a> etl.postgres.<b>script</b>(<i>pool</i>, <i>schema</i>, <i>table</i> [,<i>options</i>])

Collects data and builds up a postgres statement to insert/update data until the buffer is more than `maxBuffer` (customizable in options).  Then the maxBuffer is reached, a full sql statement is pushed downstream.   When the input stream has ended, any remaining sql statement buffer will be flushed as well.

The script stream first establishes the column names of the table being updated, and as data comes in - it uses only the properties that match column names in the table.

<a name="postgresexecute" href="#postgresexecute">#</a> etl.postgres.<b>execute</b>(<i>pool</i> [,<i>options</i>])

This component executes any incoming packets as sql statements using connections from the connection pool. The maximum concurrency is automatically determined by the postgres poolSize, using the combination of callbacks and Promises.

Example:

```js
// The following bulks data from the csv into sql statements and executes them with 
// a maximum of 4 concurrent connections

etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.postgres.script(pool,'testschema','testtable'))
  .pipe(etl.postgres.execute(pool,4))
```

#### Elasticsearch

<a name="elasticbulk" href="#elasticbulk">#</a> etl.elastic.<b>bulk</b>(<i>action</i>, <i>client</i>, [,<i>index</i>] [,<i>type</i>] [,<i>options</i>])

Transmit incoming packets to elasticsearch, setting the appropriate meta-data depending on the default action. Each incoming packet can be an array of documents (or a single document).  Each document should contain a unique `_id`.   To bulk documents together use `etl.collect(num)` above the elastic adapter.

The results are not pushed downstream unless `pushResults` is defined in the options. The body of the incoming data is included in the results, allowing for easy resubmission upon version conflicts. By defining option `pushErrors` as `true` only errors will be pushed downstream.  Maximum number of concurrent connections can be defined as option `concurrency`.  If `maxRetries` is defined in options, an error response from the server will result in retries up to the specified limit - after a wait of `retryDelay` or 30 seconds.  This can be useful for long-running upsert operations that might encounter the occasional network or timeout errors along the way.  If `debug` is defined true, the error message will be printed to console before retrying.  `maxRetries` should only be used for data with user-supplied `_id` to prevent potential duplicate records on retry.

An exponential backoff is provided by defining `backoffDelay` in options. The backoff can be capped by defining `maxBackoffDelay` and variance can be applied by defining `backoffVariance` (should be between 0-1)

If index or type are not specified when the function is called,  they will have to be supplied as `_index` and `_type` properties of each document. The bulk command first looks for `_source` in the document to use as a document body (in case the document originates from a scroll command), alternatively using the document itself as a body.

Available actions are also provided as separate api commands:

* `etl.elastic.index(client,index,type,[options])`
* `etl.elastic.update(client,index,type,[options])`
* `etl.elastic.upsert(client,index,type,[options])`
* `etl.elastic.delete(client,index,type,[options])`
* `etl.elastic.custom(client,index,type,[options])`

Example

```js
etl.file('test.csv')
  .pipe(etl.csv())
  .pipe(etl.collect(100))
  .pipe(etl.elastic.index(esClient,'testindex','testtype'))
```

Another example shows how one index can be copied to another, retaining the `_type` of each document:

```js
console.time('copy');
etl.elastic.scroll(esClient,{index: 'index.a', size: 5000})
  .pipe(etl.collect(1000))
  .pipe(etl.elastic.index(esClient,'index.b',null,{concurrency:10}))
  .promise()
  .then(function() {
    console.timeEnd('copy');
  });
```

If `custom` action is selected, each packet must be the raw metadata to be sent to elasticsearch with the optional second line stored in property `body`

Since `_type` is no longer allowed in elasticsearch > 7, it should be set as undefined for use in later versions.

#### BigQuery

<a name="bigquery" href="#bigquery">#</a> etl.bigquery.<b>insert</b>(<i>table</i>, [,<i>options</i>])

Bulk insert data into BigQuery. This function first downloads the field names for the table and then inserts the matching columns from the incoming data.  The first variable needs to be an instance of the BigQuery table class.  Options can specify the concurrency (i.e. how many concurrent insert connections are allowed);

example:

```js
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery(config);
const dataset = bigquery.dataset('my_dataset');
const table = dataset.table('my_table');

 etl.file('test.csv')
  .pipe(etl.collect(100))  // send 100 rows in each packet
  .pipe(etl.bigquery.insert(table, {concurrency: 5}));
```


### Cluster

<a name="clusterschedule" href="#clusterschedule">#</a> etl.cluster.<b>schedule</b>(<i>list</i> [,<i>num_threads</i>] [,<i>reportingInterval</i>])

Schedules a list (array) of tasks to be performed by workers.  Returns a promise on the completion of all the tasks.   Number of threads will default to number of cpus.  If reporting interval is defined - progress will be reported in console.log.Should only be run from the master thread.  

<a name="clusterprocess" href="#clusterprocess">#</a> etl.cluster.<b>process</b>(<i>data</i> <i>[callback]</i>) 

This function should be overwritten in the worker to perform each task and either return a Promise that is resolved when the task is done or call the optional callback.

<a name="clusterprocess" href="#clusterprocess">#</a> etl.cluster.<b>process</b>(<i>num</i>)

This function sends a numerical value representing progress up to the master (for reporting).  


### Utilities

<a name="tostream" href="#tostream">#</a> etl.<b>toStream</b>(<i>data</i>)

A helper function that returns a stream that is initialized by writing every element of the supplied data (if array) before being ended.  This allows for an easy transition from a known set of elements to a flowing stream with concurrency control.  The input `data` can also be supplied as a promise or a function and the resulting values will be piped to the returned stream. If the resulting value from a supplied function or promise is a stream, it will be piped downstream.

<a name="file" href="#file">#</a> etl.<b>file</b>(<i>data</i> [,<i>options</i>])

Opens up a `fileStream` on the specified file and pushes the content downstream.  Each packet has a base prototype of of either an  optional `info` object provided in options or the empty object.  The following properties are defined for each downstream packet:  `__filename`, '__path' and `text` containing incremental contents of the file.

The optional `info` object allows setting generic properties that will, through inheritance, be available in any derived packets downstream.

Example:

```js
// each packet will contain  properties context, __filename, __path and text
etl.file('text.txt',{info: {context: 'test'}})
```

<a name="tofile" href="#tofile">#</a> etl.<b>toFile</b>(<i>filename</i>)

This is a convenience wrapper for `fs.createWriteStream` that returns a `streamz` object.  This allows appending `.promise()` to capture the finish event (or error) in a promise form.

Example:

```js
etl.toStream([1,2,3,4,5])
  .pipe(etl.stringify(0,null,true))
  .pipe(etl.toFile('/tmp/test.txt'))
  .promise()
  .then(function() {
    console.log('done')
  })
  .catch(function(e) {
    console.log('error',e);
  })
```

<a name="keepopen" href="#keepopen">#</a> etl.<b>keepOpen</b>(<i>[timeout]</i>)

`etl.keepOpen([timeout])` is a passthrough component that stays open after receiving an `end` only to finally close down when no data has passed through for a period of `[timeout]`.  This can be useful for any pipelines where data from lower part of the pipeline is pushed back higher for reprocessing (for example when encountering version conflicts of database documents) - as it will avoid `write after end` error.   The default timeout is 1000ms

## Testing

Testing environment is provided using docker-compose.  

`npm test` starts docker-compose (if not already running) and executes the test suite.

You can run individual tests from the docker directly.  To enter the docker type `npm run docker`