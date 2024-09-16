const Streamz = require('streamz');
const util = require('util');
const Csv = require('csv-parser');

function Csv_parser(options) {
  if (!(this instanceof Streamz))
    return new Csv_parser(options);
  
  Streamz.call(this);
  
  this.options = options = options || {};
  this.options.transform = options.transform || {};
  this.csv = Csv(options);

  const _compile = this.csv._compile;
  const self = this;

  this.csv._compile = function() {
    if (options.sanitize)
      this.headers = this.headers.map(header => String(header)
        .trim()
        .toLowerCase()
        .replace(/\./g,'')
        .replace(/\'/g,'')
        .replace(/\s+/g,'_')
        .replace(/\u2013|\u2014/g, '-')
        .replace(/\//g,'_')
      );
        
    self.emit('headers',this.headers);
    return _compile.call(this);
  };
  this.csv.on('data',data => this._push(data));
}

util.inherits(Csv_parser,Streamz);

Csv_parser.prototype.base = {};

Csv_parser.prototype.line = 1;

Csv_parser.prototype._fn = function(d) { 
  if (d instanceof Buffer || typeof d !== 'object')
    d = Object.create({},{
      // text should be non-enumerable
      text: {
        value: d.toString('utf8'),
        writable: true,
        configurable: true
      } 
    });
  if (typeof d === 'object')
    this.base = d;
  this.csv.write(d.text || d);
};

Csv_parser.prototype._push = function(d) {
  const obj = Object.create(this.base);
  for (let key in d) {
    if (this.options.sanitize && typeof d[key] === 'string' && !d[key].trim().length) {
      d[key] = undefined;
    } else {
      const transform = this.options.transform[key];
      if (typeof transform === 'function')
        obj[key] = transform(d[key]);
      else if (transform !== null)
        obj[key] = d[key];
    }
  }

  if (this.options.addLineNumbers !== false) {
    obj.__line = ++this.line;
  }

  this.push(obj);
};

Csv_parser.prototype._flush = function(cb) {
  this.csv.end();
  setImmediate( () => Streamz.prototype._flush(cb));
};

module.exports = Csv_parser;