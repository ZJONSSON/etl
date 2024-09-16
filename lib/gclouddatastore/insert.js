var Streamz = require('streamz'),
    Promise = require('bluebird'),
    util = require('util');

function Insert(client,options) {
  if (!(this instanceof Streamz))
    return new Insert(_c,client);

  if (!client)
    throw 'CLIENT_MISSING';

  Streamz.call(this,options);
  this.client = client;
  this.options = options || {};
}

util.inherits(Insert,Streamz);

Insert.prototype._fn = function(d) {
  var self = this;
  var query = {
    key: client.key([this.options.prefix]),
    data: d
  };
  return client.insert(query)
    .then(d => {
      if (self.options.pushResult)
        return d;
    });
};

module.exports = Insert;
