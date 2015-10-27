require('../support/test_helper.js');

var assert = require('assert');
var MapConfig = require('../../lib/windshaft/models/mapconfig');

describe('mapconfig filters', function() {

    describe('aggregations', function() {

        var categoryWidgetMapConfig = {
            version: '1.5.0',
            layers: [
                {
                    type: 'mapnik',
                    options: {
                        sql: 'select * from populated_places_simple_reduced',
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

        describe('errors', function() {
            var mapConfig = MapConfig.create(categoryWidgetMapConfig);

            it('fails to apply category filter if no params are used', function() {
                assert.throws(
                    function() {
                        mapConfig.applyFilters({layers: [{
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
                        mapConfig.applyFilters({layers: [{
                            adm0name: {
                                accept: []
                            }
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Category filter expects to have at least one value in accept or reject arrays'
                        );
                        return true;
                    }
                );
            });

            it('fails to apply category filter if reject is empty', function() {
                assert.throws(
                    function() {
                        mapConfig.applyFilters({layers: [{
                            adm0name: {
                                reject: []
                            }
                        }]});
                    },
                    function(err) {
                        assert.equal(
                            err.message,
                            'Category filter expects to have at least one value in accept or reject arrays'
                        );
                        return true;
                    }
                );
            });
        });

        describe('queries with filters', function() {

            it('uses accept filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig);
                var acceptFilterMapConfig = mapConfig.applyFilters({layers: [{
                    adm0name: { // this is category filter associated to the aggregation widget
                        accept: ['Spain']
                    }
                }]});

                var acceptFilterAggregation = acceptFilterMapConfig.getWidget(0, 'adm0name');
                assert.equal(acceptFilterAggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (SELECT * FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_category_filter WHERE adm0name IN ('Spain')) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );

                // check original mapconfig keeps it right
                var aggregation = mapConfig.getWidget(0, 'adm0name');
                assert.equal(aggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );
            });

            it('uses reject filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig);

                var rejectFilterMapConfig = mapConfig.applyFilters({layers: [{
                    adm0name: { // this is a category filter associated to the aggregation widget
                        reject: ['Spain']
                    }
                }]});
                var rejectFilterAggregation = rejectFilterMapConfig.getWidget(0, 'adm0name');
                assert.equal(rejectFilterAggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (SELECT * FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_category_filter WHERE adm0name NOT IN ('Spain')) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );

                // check original mapconfig keeps it right
                var aggregation = mapConfig.getWidget(0, 'adm0name');
                assert.equal(aggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );
            });

            it('uses accept and reject filter param', function() {
                var mapConfig = MapConfig.create(categoryWidgetMapConfig);

                var acceptAndRejectFilterMapConfig = mapConfig.applyFilters({layers: [{
                    adm0name: { // this is a category filter associated to the aggregation widget
                        reject: ['Spain'],
                        accept: ['USA']
                    }
                }]});
                var acceptAndRejectFilterAggregation = acceptAndRejectFilterMapConfig.getWidget(0, 'adm0name');

                assert.equal(acceptAndRejectFilterAggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (SELECT * FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_category_filter WHERE adm0name IN ('USA') AND adm0name NOT IN ('Spain')) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );

                // check original mapconfig keeps it right
                var aggregation = mapConfig.getWidget(0, 'adm0name');
                assert.equal(aggregation.sql(),
                        "SELECT count(*) AS count, adm0name FROM" +
                        " (select * from populated_places_simple_reduced) " +
                        "_cdb_aggregation GROUP BY adm0name ORDER BY count DESC"
                );
            });
        });

    });

});

