var fs      = require('fs')
  , path    = require('path')
  , util    = require('util')
  , _ = require('underscore')
  , request = require('request')
;

function VectorTileInput(uri, cb) {
  console.log("ctor called with uri: "); console.dir(uri);
  if ( ! uri.hostname ) {
    cb(new Error("Missing datasource name"));
    return;
  }
  var dsname = uri.hostname;
  if ( ! datasources[dsname] ) {
    cb(new Error("Unknown datasource name '" + dsname + "'"));
    return;
  }
  var ds = datasources[dsname];
  console.log("datasource: " + util.inspect(ds));
  var layers = uri.path.substr(1);
  console.log("layers: " + layers);
  var tilestpl = ds.tiles.replace('{layers}', layers);
  console.log("tilesurl: " + tilestpl);

  this.datasource = ds;
  this.tilestpl = tilestpl;
  this.uri = uri;
  this.info = {
/*
    minzoom: 0,
    maxzoom: 40,
    maskLevel: 0, // ??
*/
  };
  cb(null, this);
}
var o = VectorTileInput.prototype;
o.getFile = function(uri, callback) {
  if ( uri.match('file://') ) {
    uri = uri.substr(7);
    var file = fs.readFileSync(uri);
    callback(null, file);
    return;
  }
  request({
    uri:uri,
    encoding:null
  }, function (err, res, file) {
    if ( err ) callback(err);
    else if ( res.statusCode != 200 ) {
      callback(new Error(res.statusCode + ': ' + file));
    }                                                                                                                                                  else {
      //DEBUG
      //var outpath = '/tmp/out.pbf';
      //fs.writeFileSync('/tmp/out.pbf', file, 'binary');
      //console.log("Tile saved to " + outpath);
      callback(null, file);
    }
  })
}
o.getTile = function(z,x,y,callback) {
  console.log("getTile called with z:" + z + ',x:' + x + ',y:' + y );
  var uri = this.tilestpl.replace('{z}', z).replace('{x}',x).replace('{y}',y);
  console.log("URI: " + uri);

  this.getFile(uri, function(err, tilefile) {
    callback(err, tilefile);
  });
}
o.getInfo = function(callback) {
  //console.log("getInfo called with args: "); console.dir(arguments);
  callback(null, this.info);
}

function VectorTileInputFactory(ds)
{
  if ( ! ds ) {
    throw new Error("VectorTileInputFactory needs datasources");
  }
  // NOTE: 'datasources' is module-static
  datasources = ds;
}

var o = VectorTileInputFactory.prototype;
o.getHandler = function () {
  return VectorTileInput;
};

module.exports = VectorTileInputFactory;
