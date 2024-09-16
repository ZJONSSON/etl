const etl = require('../index');
const path = require('path');
const t = require('tap');
const data = require('./data');

t.test('csv',async t => {

  t.test('parsing',async t => {
    const csv = etl.csv_parser({
      sanitize: true,
      transform: {
        dt: d => new Date(d)
      }
    });

    etl.file(path.join(__dirname,'test.csv')).pipe(csv);

    // Adjust expected values to the csv
    const expected = data.copy().map(function(d) {
      d.__line = d.__line +1;
      d.__filename = 'test.csv';

      // Clear out __path and text as they are volatile
      d.__path = undefined;
      d.text = undefined;
      
      return d;
    });

    
    const d = await csv.pipe(etl.expand()).promise();
    
    d.forEach(d => {
      d.__path = undefined;
      d.text = undefined;
    });

    t.same(d,expected,'parses data correctly');
  });

  t.test('when addLineNumbers is set to false',async t => {
    const csv = etl.csv_parser({
      sanitize: true,
      addLineNumbers: false,
      transform: {
        dt: d => new Date(d)
      }
    });

    etl.file(path.join(__dirname,'test.csv')).pipe(csv);

    // Adjust expected values to the csv
    const expected = data.copy().map(function(d) {
      d.__filename = 'test.csv';

      // Clear out __path and text as they are volatile
      d.__path = undefined;
      d.text = undefined;
      delete d.__line;
      
      return d;
    });

    const d = await csv.pipe(etl.expand()).promise();
    
    d.forEach(d => {
      d.__path = undefined;
      d.text = undefined;
    });

    t.same(d,expected,'parses data correctly');
  });
});