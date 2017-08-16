#!/usr/bin/env node

function usage() {
    console.log("--output output.mbtiles 1.mbtiles 2.mbtiles");
}

var argv = require('minimist')(process.argv.slice(2),{
    output: 'string'
});
var fs = require('fs');
var mbtiles = require('@mapbox/mbtiles');
var async = require('async');
var mapnik = require("mapnik");
var Q = require("Q");
var zlib = require("zlib");

var CONCURRENT_TILES = 2;

mbtiles.prototype.tileExists = function(z, x, y, callback) {
    y = (1 << z) - 1 - y;
    // console.log("tileExists");

    var sql = 'SELECT count(1) as count FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?';
    var mbtiles = this;
    var params = [z, x, y];
    this._db.get(sql, params, function(err, row) {
        var exists = row.count != 0;
        callback(err, exists);
    });
};

var outputPath = argv.output;
var inputs = argv._;

if(!outputPath || !inputs || !inputs.length || inputs.length < 2) {
    usage();
    process.exit(1);
}

if(fs.existsSync(outputPath)) {
    console.log("Output path already exists");
    process.exit(1);
}

try {
    inputs.forEach(function (f) {
        fs.statSync(f).isFile();
    });
} catch(e) {
    return console.error(e);
}

var inputTiles = [];

console.log("opening inputs");
async.each(inputs, function(path, callback) {
    new mbtiles(path, function(err, tiles) {
        if(err) throw err;
        inputTiles.push(tiles);
        callback();
    });
}, function(err) {
    console.log("opening output");
    if(err) throw err;
    new mbtiles(outputPath, function(err, tiles) {
        if(err) throw err;
        merge(tiles, inputTiles);
    });
});


function merge(output, inputs) {
    console.log("Starting merge");
    async.waterfall([
        function(callback) {
            console.log("copying info");
            inputs[0].getInfo(function(err, info){
                if(err) return callback(err);
                output.startWriting(function(err){
                    if(err) return callback(err);
                    output.putInfo(info, function(err){
                        if(err) return callback(err);
                        output.stopWriting(function(err){
                            callback(err);
                        });
                    });
                });
            });
        },
        function(callback){
            console.log("merging inputs");
            async.eachOfLimit(inputs, 1, function(input, index, eachCallback){
                console.log("merge input " + (index + 1) + "/" + inputs.length);
                output.startWriting(function(err){
                    if(err) return eachCallback(err);
                    mergeInput(output, input, inputs, function(err){
                        if(err) return eachCallback(err);
                        console.log("Merging " + (index + 1) + "/" + inputs.length + " finished, calling stopWriting");
                        output.stopWriting(function(err){
                            eachCallback(err);
                        })
                    });
                });
            }, function(err) {
                if(err) console.log("error merging input", err);
                callback(err);
            });
        },
    ], function(err){
        console.log("done");
        if(err){
            console.log(err);
            throw err;
        }
    });
}

function mergeInput(output, input, inputs, callback) {
    //Iterate tiles in input
    var maxzoom = null;
    var sql = "SELECT zoom_level AS z, tile_column AS x, tile_row AS y FROM tiles";
    var params = [];
    if(maxzoom) {
        sql += " WHERE zoom_level < ?";
        params.push(maxzoom);
    }
    input._db.all(sql, params, function(err, rows) {
        console.log("Processing " + rows.length + " rows from input");
        if(err) return callback(err);
        async.eachOfLimit(rows, CONCURRENT_TILES, function(row, index, eachCallback){
            var x = row.x;
            var z = row.z;
            var y = flipY(row.y, z);

            //check if tile exists in output
            output.tileExists(z, x, y, function(err, existing) {
                if(existing) { 
                    console.log("Tile exists, skipping");
                    eachCallback();
                } else { //tile does not yet exist in output, so merge it
                    mergeTile(output, inputs, x, y, z, function(err) {
                        eachCallback(err);
                    });
                }
            });
        }, function(err){
            if(err) {
                console.log("Error iterating tiles: ", err);
            }
            callback(err);
        });
    });
}

function mergeTile(output, inputs, x, y, z, callback) {
    //Fetch tile from each input
    var promises = [];
    inputs.forEach(function(input){
        var deferred = Q.defer();
        promises.push(deferred.promise);
        input.getTile(z, x, y, function(err, data) {
            if(data) {
                try {
                    var inputTile = new mapnik.VectorTile(z, x, y);
                    inputTile.setData(data);
                    deferred.resolve(inputTile);
                } catch (err) {
                    deferred.reject(err);
                }
            } else {
                deferred.resolve(null);
            }
        });
    });

    //Merge all the input tiles
    Q.all(promises).then(function(tiles) {
        var tilesWithData = [];
        for(var i  = 0; i < tiles.length; i++) {
            if(tiles[i]) {
                tilesWithData.push(tiles[i]);
            }
        }

        var outputTile = null;
        if(tilesWithData.length == 1) {
            console.log("Using existing tile for " + z + "/" + x + "/" + y);
            outputTile = tilesWithData[0];
        } else { // > 1
            outputTile = new mapnik.VectorTile(z, x, y);
            console.log("Merging " + tilesWithData.length + " tiles to " + z + "/" + x + "/" + y);
            try {
                outputTile.compositeSync(tilesWithData);
            } catch (err) {
                console.log(err);
                return callback(err);
            }
        }

        //write to output
        zlib.gzip(outputTile.getData(), function(err, data) {
            output.putTile(z, x, y, data, function(err) {
                callback(err);
            });
        });
    }, function(err) {
        if(err) console.log(err);
        callback(err);
    });
}

function flipY(y, z) {
    return (1 << z) - 1 - y;
}
