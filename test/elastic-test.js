const etl = require('../index');
const data = require('./data');
const Promise = require('bluebird');
const elasticsearch = require('@elastic/elasticsearch');
const t = require('tap');

const client = new elasticsearch.Client({
  node: 'http://elasticsearch:9200'
});

function convertHits(d) {
  return d.map(d => {
    d = d._source;
    d.dt = new Date(d.dt);
    return d;
  })
  .sort((a,b) => a.__line - b.__line);
}

t.test('elastic', async t => {
  await client.indices.delete({index:'test'}).catch(Object);
  await client.indices.delete({index:'testretries'}).catch(Object);

  t.test('#getMeta', t => {
    const bulk = etl.elastic.bulk('index', {});
    const d = {_id: '1', parent: 'testparent', routing: 'testrouting', _index: 'testindex', _type: 'testtype'};
    const metaData = bulk.getMeta(d);
    t.ok(metaData.hasOwnProperty('index'),'returns Object with key "index"');
    t.same(metaData.index.parent,'testparent','includes parent=testparent');
    t.same(metaData.index.routing,'testrouting','includes routing=testrouting');
    t.same(metaData.index._id,'1','includes _id=1');
    t.same(metaData.index._type,'testtype','includes type=testtype');
    t.same(metaData.index._index,'testindex','includes index=testindex');
    t.end();
  });

  t.test('pipe into etl.elastic.index()',async t => {
    let i = 0;
    const upsert = etl.elastic.index(client,'test',undefined,{pushResults:true});

    const d = await data.stream()
      .pipe(etl.map(d => {
        d._id = i++;
        return d;
      }))
      .pipe(etl.collect(100))
      .pipe(upsert)
      .promise();

    t.same(d[0].items.length,3,'record count matches');
    t.same(d[0].items[0].body,data.data[0],'data matches');
  });

  t.test('retreive data with client.search()', async t => {
    await Promise.delay(2000); 
    let d = await client.search({index:'test'});
    if (d.body) d = d.body;
    const values = convertHits(d.hits.hits);
    t.same(values,data.data,'data matches');
  });

  t.test('etl.elastic.find()', async t => {
    const find = etl.elastic.find(client);
    find.end({index:'test'});

    let d = await find.promise();  
    const values = convertHits(d);
    if (d.body) d = d.body;
    t.same(values,data.data,'returns original data');
  });

  t.test('etl.elastic.scroll()', async t => {
    const scroll = etl.elastic.scroll(client,{index: 'test', size: 1},{ highWaterMark: 0 });
    // setting highWaterMark to zero and size = 1 allows us to test for backpressure
    // a missing scroll_id would indicate that scrolling has finished pre-emptively
    const d = await scroll.pipe(etl.map(d => {
      t.ok(scroll.scroll_id && scroll.scroll_id.length,'Scroll id available - backpressure is managed');
      return Promise.delay(200).then(() => d);
    },{highWaterMark: 0}))
    .promise();

    t.same(scroll.scroll_id,undefined,'scrolling has finished');  // scrolling has finished
    t.same(convertHits(d),data.data,'returns original data');
  });

  t.test('No retry on mapping exception', async t => {
    const upsert = etl.elastic.index(client,'testretries',undefined,{maxRetries: 1, retryDelay:1, pushErrors: true});
    let results = upsert.pipe(etl.map()).promise();
    upsert.write({number:2});
    upsert.write({number: 'not a number'});
    upsert.end();

    results = await results;
    t.same(results[0][0].error.type, 'document_parsing_exception');
    t.end();
  });
});
