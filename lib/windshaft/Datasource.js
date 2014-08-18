var fs      = require('fs')
  , path    = require('path')
  , util    = require('util')
  , _ = require('underscore')
  , request = require('request')
  , zlib = require('zlib')
  , tilelive = require('tilelive')
  , mapnik = require('mapnik')
;

function StaticVectorTileSource(uri, cb) {
  var cfg = uri.mapcfg;

  var layers = [];
  for ( var i=0; i<cfg.layers.length; ++i ) {
    var lyr = cfg.layers[i];
    var lyrname = lyr.options.sql;
    layers.push(lyrname);
    // rename layers to their vector datasource
    // layer name 
    //var pat = RegExp("layer"+i, 'g');
    //uri.xml = uri.xml.replace(pat, lyrname); 
  }
  layers = layers.join(',');

  var ds = uri.datasource;
  var layers = uri.path;
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

var o = StaticVectorTileSource.prototype;
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
      //var outpath = '/tmp/' + uri.replace(/\//g, '.') + '.pbf';
      //fs.writeFileSync(outpath, file, 'binary');
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
    if ( err ) { callback(err); return; }
    if ( tilefile[0] == 0x1A ) {
      // See https://github.com/mapbox/tilelive-vector/issues/71
      //console.log("TILE IS NOT COMPRESSED!")
      zlib.deflate(tilefile, function(err, data) {
        callback(err, data);
      });
      return;
    }
    callback(null, tilefile);
  });
};
o.getInfo = function(callback) {
  //console.log("getInfo called with args: "); console.dir(arguments);
  callback(null, this.info);
};


//-------------------------------------------------------------

function PgsqlVectorTileSource(uri, cb) {
  var cfg = uri.mapcfg;

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

var o = PgsqlVectorTileSource.prototype;
o.getInfo = function(callback) {
  //console.log("getInfo called with args: "); console.dir(arguments);
  callback(null, this.info);
};
o.getTile = function(z,x,y,callback) {
  console.log("getTile called with z:" + z + ',x:' + x + ',y:' + y );

  // TODO:
  //  For each layer, construct a VectorTile
  //  Merge all VectorTiles togeter, return the tile
  callback(new Error("PgsqlVectorTileSource.getTile not implemented yet"));
};

//-------------------------------------------------------------

function Datasource(uri, cb) {
  console.log("ctor called with uri: "); console.dir(uri);
  if ( ! uri.mapcfg ) {
    cb(new Error("Missing mapconfig in Datasource constructor"));
    return;
  }
  var dsname = uri.mapcfg.datasource;
  var ds = datasources[dsname];
  if ( ! ds ) {
    cb(new Error("Unknown datasource name '" + dsname + "'"));
    return;
  }
  console.log("datasource: " + util.inspect(ds));

  var source;
  if ( ds.tiles ) {
    new StaticVectorTileSource(uri, cb);
  }
  else if ( ds.postgres ) {
    new PgsqlVectorTileSource(uri, cb);
  }
  else {
    cb(new Error("Unknown datasource type"));
    return;
  }
}


function DatasourceFactory(ds)
{
/*
  if ( ! ds ) {
    throw new Error("DatasourceFactory needs datasources");
  }
*/
  // NOTE: 'datasources' is module-static
  datasources = ds;
}

var o = DatasourceFactory.prototype;
o.getHandler = function () {
  return Datasource;
};

o.loadURI = function(cfg, uri, callback) {
  var dsname = cfg.datasource;
  var ds = datasources[dsname];
  if ( ! ds ) {
    tilelive.load(uri, callback);
    return;
  }

  var type = ds.type || 'static';
  if ( ds.tiles ) {
    // TODO: setup token-specific configuration and
    //       reference it as part of the source uri
    var layers = [];
    for ( var i=0; i<cfg.layers.length; ++i ) {
      var lyr = cfg.layers[i];
      var lyrname = lyr.options.sql;
      layers.push(lyrname);
      // rename layers to their vector datasource
      // layer name 
      var pat = RegExp("layer"+i, 'g');
      uri.xml = uri.xml.replace(pat, lyrname); 
    }
    layers = layers.join(',');
  }

  uri.source = {
    protocol: 'windshaft:',
    datasource: ds,
    //hostname: dsname,
    mapcfg: cfg
    //path: layers
  }

  // strip Datasource tags, there's a single one now
  uri.xml = uri.xml.replace(/\<Datasource\>[\s\S]*?\<\/Datasource\>/g, '');

  // VectorTile tiles are always in epsg:3857
  // TODO: make it safer, change grainstore configuration
  //       prior to get here ?
  uri.xml = uri.xml.replace(/epsg:4326/g, 'epsg:3857'); 

  uri.protocol = 'vector:';

  // hand off to tilelive to create a renderer
  tilelive.load(uri, callback);
};

module.exports = DatasourceFactory;
