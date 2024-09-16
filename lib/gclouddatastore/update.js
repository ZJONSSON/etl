var Streamz = require('streamz'),
    Promise = require('bluebird'),
    util = require('util');

function Update(client,keys,options) {
  if (!(this instanceof Streamz))
    return new Update(_c,client);

  if (!client)
    throw 'CLIENT_MISSING';

  if (keys === undefined)
    throw new Error('Missing Keys');

  Streamz.call(this,options);
  this.client = client;
  this.options = options || {};
  this.keys = [].concat(keys);
}

util.inherits(Update,Streamz);

Update.prototype._fn = function(d) {
  var self = this;
  var query = this.keys.reduce((o,key) => {
    if (d[key] === undefined)
      throw new Error('Key not found in data');
    return o.push({
      key: self.client.key([self.options.prefix,key]),
      data: d[key]
    });
  },[]);
  return (this.options.upsert ? client.upsert(query) : client.update(query))
    .then(d => {
      if (self.options.pushResult)
        return d;
    });
};

module.exports = Update;
