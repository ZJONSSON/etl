const etl = require('../index');
const data = require('./data');
const t = require('tap');

t.test('fixed layout', {jobs: 10}, async t => {
  t.test('defined as object', async t => {
    const d = await data.stream()
      .pipe(etl.map(d => ({text: d.text, __filename: d.__filename})))
      .pipe(etl.fixed(data.layout))
      .pipe(etl.expand())
      .promise();

    t.same(d,data.data,'splits incoming data into columns');
  });
  

  t.test('defined as an array', async t => {
    const layout = Object.keys(data.layout).map(key => {
      let val = data.layout[key];
      if (!isNaN(val)) val = {length:val};
      val.field = key;
      return val;
    });

    const d = await data.stream()
      .pipe(etl.map(d => ({text: d.text, __filename: d.__filename})))
      .pipe(etl.fixed(layout))
      .pipe(etl.expand())
      .promise();
      
    t.same(d,data.data,'splits incoming data into columns');
  });  
});