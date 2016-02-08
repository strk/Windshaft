require('../support/test_helper.js');

var assert = require('assert');
var MapConfig = require('../../lib/windshaft/models/mapconfig');

describe('mapconfig filters', function() {

    var layerSql = 'select * from populated_places_simple_reduced';

    describe('aggregations', function() {

        function categoryWidgetMapConfig() {
            return {
                version: '1.5.0',
                layers: [
                    {
                        type: 'mapnik',
                        options: {
                            sql: layerSql,
                            cartocss: '#layer0 { marker-fill: red; marker-width: 10; }',
                            cartocss_version: '2.0.1',
                            widgets: {
                                adm0name: {
                                    type: 'aggregation',
                                    options: {
                                        aggregation: 'count',
                                        column: 'adm0name'
                                    }
                                }
                            }
                        }
                    }
                ]
            };
        }

        describe('errors', function() {
            var mapConfig = MapConfig.create(categoryWidgetMapConfig());

            it('fails to apply category filter if no params are used', function() {
                assert.throws(
                    function() {
                        mapConfig.setFiltersParams({layers: [{
                            adm0name: {}
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Category filter expects at least one array in accept or reject params'
                        );
                        return true;
                    }
                );
            });

            it('fails to apply category filter if accept is empty', function() {
                assert.throws(
                    function() {
                        mapConfig.setFiltersParams({layers: [{
                            adm0name: {
                                accept: [],
                                reject: []
                            }
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Category filter expects one value either in accept or reject params when both are provided'
                        );
                        return true;
                    }
                );
            });
        });

        describe('queries with filters', function() {

            it('uses accept filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig());
                var mapConfigId = mapConfig.id();

                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a category filter associated to the aggregation widget
                        accept: ['Spain']
                    }
                }]});

                assert.equal(mapConfig.getLayer(0).options.sql,
                    [
                        'SELECT *',
                        'FROM (select * from populated_places_simple_reduced) _cdb_category_filter',
                        'WHERE adm0name IN ($escape_0$Spain$escape_0$)'
                    ].join('\n')
                );

                assert.notEqual(mapConfig.id(), mapConfigId);
            });

            it('uses reject filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig());

                var mapConfigId = mapConfig.id();

                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a category filter associated to the aggregation widget
                        reject: ['Spain']
                    }
                }]});

                assert.equal(mapConfig.getLayer(0).options.sql,
                    [
                        'SELECT *',
                        'FROM (select * from populated_places_simple_reduced) _cdb_category_filter',
                        'WHERE adm0name NOT IN ($escape_0$Spain$escape_0$)'
                    ].join('\n')
                );

                assert.notEqual(mapConfig.id(), mapConfigId);
            });

            it('uses accept and reject filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig());

                var mapConfigId = mapConfig.id();

                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a category filter associated to the aggregation widget
                        reject: ['Spain'],
                        accept: ['USA']
                    }
                }]});

                assert.equal(mapConfig.getLayer(0).options.sql,
                    [
                        'SELECT *',
                        'FROM (select * from populated_places_simple_reduced) _cdb_category_filter',
                        'WHERE adm0name IN ($escape_0$USA$escape_0$) AND adm0name NOT IN ($escape_0$Spain$escape_0$)'
                    ].join('\n')
                );

                assert.notEqual(mapConfig.id(), mapConfigId);
            });
        });

    });

    describe('range', function() {
        function histogramWidgetMapConfig() {
            return {
                version: '1.5.0',
                layers: [
                    {
                        type: 'mapnik',
                        options: {
                            sql: layerSql,
                            cartocss: '#layer0 { marker-fill: red; marker-width: 10; }',
                            cartocss_version: '2.0.1',
                            widgets: {
                                adm0name: {
                                    type: 'histogram',
                                    options: {
                                        column: 'adm0name'
                                    }
                                }
                            }
                        }
                    }
                ]
            };
        }

        describe('errors', function() {
            var mapConfig = MapConfig.create(histogramWidgetMapConfig());

            it('fails to apply range filter if no params are used', function() {
                assert.throws(
                    function() {
                        mapConfig.setFiltersParams({layers: [{
                            adm0name: {}
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Range filter expect to have at least one value in min or max numeric params'
                        );
                        return true;
                    }
                );
            });

            it('fails to apply range filter if min is not a number', function() {
                assert.throws(
                    function() {
                        mapConfig.setFiltersParams({layers: [{
                            adm0name: {
                                min: 'wadus'
                            }
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Range filter expect to have at least one value in min or max numeric params'
                        );
                        return true;
                    }
                );
            });

            it('fails to apply range filter if max is not a number', function() {
                assert.throws(
                    function() {
                        mapConfig.setFiltersParams({layers: [{
                            adm0name: {
                                max: 'wadus'
                            }
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Range filter expect to have at least one value in min or max numeric params'
                        );
                        return true;
                    }
                );
            });
        });

        describe('queries with filters', function() {

            it('uses min filter param', function() {
                var mapConfig = MapConfig.create(histogramWidgetMapConfig());
                var mapConfigId = mapConfig.id();
                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a range filter associated to the histogram widget
                        min: 0
                    }
                }]});

                assert.ok(mapConfig.getLayer(0).options.sql.match(/_cdb_range_filter WHERE adm0name >= 0/));
                assert.notEqual(mapConfig.id(), mapConfigId);
            });

            it('uses max filter param', function() {
                var mapConfig = MapConfig.create(histogramWidgetMapConfig());
                var mapConfigId = mapConfig.id();
                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a range filter associated to the histogram widget
                        max: 100
                    }
                }]});

                assert.ok(mapConfig.getLayer(0).options.sql.match(/_cdb_range_filter WHERE adm0name <= 100/));
                assert.notEqual(mapConfig.id(), mapConfigId);
            });

            it('uses min and max filter params', function() {
                var mapConfig = MapConfig.create(histogramWidgetMapConfig());
                var mapConfigId = mapConfig.id();
                assert.equal(mapConfig.getLayer(0).options.sql, layerSql);

                mapConfig.setFiltersParams({layers: [{
                    adm0name: { // this is a range filter associated to the histogram widget
                        min: 0,
                        max: 100
                    }
                }]});

                assert.ok(
                    mapConfig.getLayer(0).options.sql.match(/_cdb_range_filter WHERE adm0name BETWEEN 0 AND 100/)
                );
                assert.notEqual(mapConfig.id(), mapConfigId);
            });
        });

    });

});

