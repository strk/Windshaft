// FLUSHALL Redis before starting

var   assert        = require('../support/assert')
    , tests         = module.exports = {}
    , _             = require('underscore')
    , querystring   = require('querystring')
    , fs            = require('fs')
    , redis         = require('redis')
    , th            = require('../support/test_helper')
    , Step          = require('step')
    , mapnik        = require('mapnik')
    , Windshaft     = require('../../lib/windshaft')
    , ServerOptions = require('../support/server_options')
    , http          = require('http');

suite('mvt', function() {

    ////////////////////////////////////////////////////////////////////
    //
    // SETUP
    //
    ////////////////////////////////////////////////////////////////////

    var res_serv; // resources server
    var res_serv_port = 8033; // FIXME: make configurable ?

    if ( ! ServerOptions.mapnik ) ServerOptions.mapnik = {};
    ServerOptions.mapnik.vector_datasources = {
      'fs': {
        tiles: "file://test/fixtures/vector.coastline.1.1.1.pbfz"
      },
      'fs2': {
        tiles: "file://test/fixtures/vector.coastline.1.1.1.pbf"
      },
      'http': {
        tiles: "http://localhost:" + res_serv_port + "/vector.coastline.1.1.1.pbfz"
      },
      'sql': {
        postgres: true
      }
    };
    var IMAGE_EQUALS_TOLERANCE_PER_MIL = 20;

    var server = new Windshaft.Server(ServerOptions);
    server.setMaxListeners(0);
    var redis_client = redis.createClient(ServerOptions.redis.port);

    checkCORSHeaders = function(res) {
      var h = res.headers['access-control-allow-headers'];
      assert.ok(h);
      assert.equal(h, 'X-Requested-With, X-Prototype-Version, X-CSRF-Token');
      var h = res.headers['access-control-allow-origin'];
      assert.ok(h);
      assert.equal(h, '*');
    };

    suiteSetup(function(done) {

      // Check that we start with an empty redis db 
      redis_client.keys("*", function(err, matches) {
          assert.equal(matches.length, 0, "redis keys present at setup time:\n" + matches.join("\n"));
      });

      // Start a server to test external resources
      res_serv = http.createServer( function(request, response) {
          var filename = __dirname + '/../fixtures' + request.url; 
          fs.readFile(filename, "binary", function(err, file) {
            if ( err ) {
              response.writeHead(404, {'Content-Type': 'text/plain'});
              console.log("File '" + filename + "' not found");
              response.write("404 Not Found\n");
            } else {
              response.writeHead(200);
              response.write(file, "binary");
            }
            response.end();
          });
      });
      res_serv.listen(res_serv_port, done);

    });

    test("simple", function(done) {

      var layergroup =  {
        // TODO: increment minor version, for 'datasource' support 
        version: '1.1.0',
        datasource: 'fs',
        layers: [
           { options: {
               sql: 'coastline',
               cartocss: '#layer { line-color:black; }', 
               cartocss_version: '2.0.1'
             } }
        ]
      };

      var expected_token; 
      Step(
        function create_map()
        {
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup?'
                + querystring.stringify({
                    'config': JSON.stringify(layergroup)
                  }),
              method: 'GET',
              headers: {'Content-Type': 'application/json' }
          }, {}, function(res) { next(null, res); });
        },
        function check_map(err, res) {
          if ( err ) throw err;
          assert.equal(res.statusCode, 200, res.statusCode + ':' + res.body);
          // CORS headers should be sent with response
          // from layergroup creation via GET
          // see https://github.com/CartoDB/Windshaft/issues/92
          checkCORSHeaders(res);
          var parsedBody = JSON.parse(res.body);
          if ( expected_token ) assert.deepEqual(parsedBody, {layergroupid: expected_token, layercount: 2});
          else expected_token = parsedBody.layergroupid;
          return null;
        },
        function get_tile(err)
        {
          if ( err ) throw err;
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup/' + expected_token
                + '/1/1/1.png',
              method: 'GET',
              encoding: 'binary'
          }, {}, function(res, err) {
              next(err, res);
          });
        },
        function check_tile(err, res)
        {
          if ( err ) throw err;
          var next = this;
          assert.equal(res.statusCode, 200, res.body);
          assert.equal(res.headers['content-type'], "image/png");
          assert.imageEqualsFile(res.body,
              './test/fixtures/vector.coastline.1.1.1.png',
              IMAGE_EQUALS_TOLERANCE_PER_MIL, this);
        },
        function finish(err) {
          var errors = [];
          if ( err ) {
            if ( ! expected_token ) { done(err); return; }
            errors.push('' + err);
          }
          redis_client.exists("map_cfg|" +  expected_token, function(err, exists) {
              if ( err ) errors.push(err.message);
              assert.ok(exists, "Missing expected token " + expected_token + " from redis");
              redis_client.del("map_cfg|" +  expected_token, function(err) {
                if ( err ) errors.push(err.message);
                if ( errors.length ) done(new Error(errors));
                else done(null);
              });
          });
        }
      );
    });

    test("http", function(done) {

      var layergroup =  {
        // TODO: increment minor version, for 'datasource' support 
        version: '1.1.0',
        datasource: 'http',
        layers: [
           { options: {
               sql: 'coastline',
               cartocss: '#layer { line-color:black; }', 
               cartocss_version: '2.0.1'
             } }
        ]
      };

      var expected_token; 
      Step(
        function create_map()
        {
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup?'
                + querystring.stringify({
                    'config': JSON.stringify(layergroup)
                  }),
              method: 'GET',
              headers: {'Content-Type': 'application/json' }
          }, {}, function(res) { next(null, res); });
        },
        function check_map(err, res) {
          if ( err ) throw err;
          assert.equal(res.statusCode, 200, res.statusCode + ':' + res.body);
          // CORS headers should be sent with response
          // from layergroup creation via GET
          // see https://github.com/CartoDB/Windshaft/issues/92
          checkCORSHeaders(res);
          var parsedBody = JSON.parse(res.body);
          if ( expected_token ) assert.deepEqual(parsedBody, {layergroupid: expected_token, layercount: 2});
          else expected_token = parsedBody.layergroupid;
          return null;
        },
        function get_tile(err)
        {
          if ( err ) throw err;
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup/' + expected_token
                + '/1/1/1.png',
              method: 'GET',
              encoding: 'binary'
          }, {}, function(res, err) {
              next(err, res);
          });
        },
        function check_tile(err, res)
        {
          if ( err ) throw err;
          var next = this;
          assert.equal(res.statusCode, 200, res.body);
          assert.equal(res.headers['content-type'], "image/png");
          assert.imageEqualsFile(res.body,
              './test/fixtures/vector.coastline.1.1.1.png',
              IMAGE_EQUALS_TOLERANCE_PER_MIL, this);
        },
        function finish(err) {
          var errors = [];
          if ( err ) {
            if ( ! expected_token ) { done(err); return; }
            errors.push('' + err);
          }
          redis_client.exists("map_cfg|" +  expected_token, function(err, exists) {
              if ( err ) errors.push(err.message);
              assert.ok(exists, "Missing expected token " + expected_token + " from redis");
              redis_client.del("map_cfg|" +  expected_token, function(err) {
                if ( err ) errors.push(err.message);
                if ( errors.length ) done(new Error(errors));
                else done(null);
              });
          });
        }
      );
    });

    test("uncompressed", function(done) {

      var layergroup =  {
        // TODO: increment minor version, for 'datasource' support 
        version: '1.1.0',
        datasource: 'fs2',
        layers: [
           { options: {
               sql: 'coastline',
               cartocss: '#layer { line-color:black; line-width:1; }', 
               cartocss_version: '2.0.1'
             } }
        ]
      };

      var expected_token; 
      Step(
        function create_map()
        {
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup?'
                + querystring.stringify({
                    'config': JSON.stringify(layergroup)
                  }),
              method: 'GET',
              headers: {'Content-Type': 'application/json' }
          }, {}, function(res) { next(null, res); });
        },
        function check_map(err, res) {
          if ( err ) throw err;
          assert.equal(res.statusCode, 200, res.statusCode + ':' + res.body);
          // CORS headers should be sent with response
          // from layergroup creation via GET
          // see https://github.com/CartoDB/Windshaft/issues/92
          checkCORSHeaders(res);
          var parsedBody = JSON.parse(res.body);
          if ( expected_token ) assert.deepEqual(parsedBody, {layergroupid: expected_token, layercount: 2});
          else expected_token = parsedBody.layergroupid;
          return null;
        },
        function get_tile(err)
        {
          if ( err ) throw err;
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup/' + expected_token
                + '/1/1/1.png',
              method: 'GET',
              encoding: 'binary'
          }, {}, function(res, err) {
              next(err, res);
          });
        },
        function check_tile(err, res)
        {
          if ( err ) throw err;
          var next = this;
          assert.equal(res.statusCode, 200, res.body);
          assert.equal(res.headers['content-type'], "image/png");
          assert.imageEqualsFile(res.body,
              './test/fixtures/vector.coastline.1.1.1.png',
              IMAGE_EQUALS_TOLERANCE_PER_MIL, this);
        },
        function finish(err) {
          var errors = [];
          if ( err ) {
            if ( ! expected_token ) { done(err); return; }
            errors.push('' + err);
          }
          redis_client.exists("map_cfg|" +  expected_token, function(err, exists) {
              if ( err ) errors.push(err.message);
              assert.ok(exists, "Missing expected token " + expected_token + " from redis");
              redis_client.del("map_cfg|" +  expected_token, function(err) {
                if ( err ) errors.push(err.message);
                if ( errors.length ) done(new Error(errors));
                else done(null);
              });
          });
        }
      );
    });

    test.skip("postgres", function(done) {

      var layergroup =  {
        // TODO: increment minor version, for 'datasource' support 
        version: '1.0.1',
        global_cartocss_version: '2.0.2',
        datasource: 'sql',
        layers: [
           { options: {
               sql: "select st_setsrid('LINESTRING(-60 -60,-60 60)'::geometry, 4326) as the_geom",
               cartocss: '#layer { line-width:16; line-color:#ff0000; }'
             } },
           { options: {
               sql: "select st_setsrid('LINESTRING(-100 0,100 0)'::geometry, 4326) as the_geom",
               cartocss: '#layer { line-width:16; line-color:#00ff00; }'
             } },
           { options: {
               sql: "select st_setsrid('LINESTRING(60 -60,60 60)'::geometry, 4326) as the_geom",
               cartocss: '#layer { line-width:16; line-color:#0000ff; }'
             } }
        ]
      };

      var expected_token; // = "32994445c0a4525432fcd7013bf6524c";
      Step(
        function do_post()
        {
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup',
              method: 'POST',
              headers: {'Content-Type': 'application/json' },
              data: JSON.stringify(layergroup)
          }, {}, function(res) {
            try {
              assert.equal(res.statusCode, 200, res.body);
              var parsedBody = JSON.parse(res.body);
              if ( expected_token ) assert.deepEqual(parsedBody, {layergroupid: expected_token, layercount: 3});
              else expected_token = parsedBody.layergroupid;
              next(null, res);
            } catch (err) { next(err); }
          });
        },
        function do_get_tile(err)
        {
          if ( err ) throw err;
          var next = this;
          assert.response(server, {
              url: '/database/windshaft_test/layergroup/' + expected_token + '/0/0/0.png',
              method: 'GET',
              encoding: 'binary'
          }, {}, function(res) {
              assert.equal(res.statusCode, 200, res.body);
              assert.equal(res.headers['content-type'], "image/png");
              assert.imageEqualsFile(res.body, './test/fixtures/test_table_0_0_0_multilayer4.png', IMAGE_EQUALS_TOLERANCE_PER_MIL, function(err) {
                  next(err);
              });
          });
        },
        function finish(err) {
          var errors = [];
          if ( err ) errors.push(err.message);
          redis_client.exists("map_cfg|" +  expected_token, function(err, exists) {
              if ( err ) errors.push(err.message);
              assert.ok(exists, "Missing expected token " + expected_token + " from redis");
              redis_client.del("map_cfg|" +  expected_token, function(err) {
                if ( err ) errors.push(err.message);
                if ( errors.length ) done(new Error(errors));
                else done(null);
              });
          });
        }
      );
    });

    ////////////////////////////////////////////////////////////////////
    //
    // TEARDOWN
    //
    ////////////////////////////////////////////////////////////////////

    suiteTeardown(function(done) {

      // Close the resources server
      res_serv.close();

      // Check that we left the redis db empty
      redis_client.keys("*", function(err, matches) {
          try {
            assert.equal(matches.length, 0, "Left over redis keys:\n" + matches.join("\n"));
          } catch (err2) {
            if ( err ) err.message += '\n' + err2.message;
            else err = err2;
          }
          redis_client.flushall(function() {
            done(err);
          });
      });

    });

});

