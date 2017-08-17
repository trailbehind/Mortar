#!/usr/bin/env node

function usage() {
    console.log("--output output.mbtiles 1.mbtiles 2.mbtiles");
}

var argv = require('minimist')(process.argv.slice(2),{
    output: 'string',
    verbose: 'boolean',
    maxzoom: 'integer'
});
var fs = require('fs');
var mbtiles = require('@mapbox/mbtiles');
var async = require('async');
var mapnik = require("mapnik");
var Q = require("Q");
var zlib = require("zlib");
var ProgressBar = require('progress');

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
var verbose = argv.verbose;
var maxzoom = argv.maxzoom || 22;
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
        merge(tiles, inputTiles, verbose, maxzoom);
    });
});


function merge(output, inputs, verbose, maxzoom) {
    console.log("Starting merge");
    async.waterfall([
        function(callback) {
            console.log("Loading infos");
            async.each(inputs, function(input, eachCallback){
                input.getInfo(function(err, info){
                    if(err) return eachCallback(err);
                    input.info = info;
                    eachCallback(null);
                });
            }, function(err){
                callback(err);
            });
        },
        function(callback) {
            console.log("Merging infos");
            var info = inputs.map(function(input){
                return input.info;
            }).reduce(function(a, b, i){
                return mergeInfo(a, b, i);
            });
            callback(null, info);
        },
        function(info, callback) {
            console.log("writing info");
            output.startWriting(function(err){
                if(err) return callback(err);
                output.putInfo(info, function(err){
                    if(err) return callback(err);
                    output.stopWriting(function(err){
                        callback(err);
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
                    mergeInput(output, input, inputs, verbose, maxzoom, function(err){
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

function mergeInfo(a, b) {
    var info = {};
    if(a.attribution || b.attribution) {
        if (a.attribution === b.attribution) {
            info.attribution = a.attribution;
        } else {
            info.attribution = [a.attribution, b.attribution].join(", ");
        }
    }

    if (a.description === b.description) {
        info.description = a.description;
    } else {
        info.description = [a.description, b.description].join(", ");
    }

    info.bounds = [
        Math.min(a.bounds[0], b.bounds[0]),
        Math.min(a.bounds[1], b.bounds[1]),
        Math.max(a.bounds[2], b.bounds[2]),
        Math.max(a.bounds[3], b.bounds[3])
    ];

    info.maxzoom = Math.max(a.maxzoom, b.maxzoom);
    info.minzoom = Math.min(a.minzoom, b.minzoom);  
    info.center = [(info.bounds[0] + info.bounds[2])/2, (info.bounds[1] + info.bounds[3]), Math.round((info.maxzoom + info.minzoom)/2)];
    info.format = a.format;
    info.version = a.version;
    info.name = [a.name, b.name].join(" + ");
    
    var vectorLayers = {};
    [a, b].forEach(function(source){
        if(source.vector_layers) {
            source.vector_layers.forEach(function(l){
                if(vectorLayers[l.id]) {
                    vectorLayers[l.id] = mergeLayerDefinition(vectorLayers[l.id], l);
                } else {
                    vectorLayers[l.id] = l;
                }
            });
        }
    });

    info.vector_layers = Object.keys(vectorLayers).map(function(k){return vectorLayers[k]});
    info.id = [a.id, b.id].join(",");

    return info;
}

function mergeLayerDefinition(a, b) {
    var layer = {
        id: a.id,
    };
    layer.minzoom = Math.min(a.minzoom, b.minzoom);
    layer.maxzoom = Math.max(a.maxzoom, b.maxzoom);
    if (a.description === b.description) {
        layer.description = a.description;
    } else {
        layer.description = [a.description, b.description].join(", ");
    }

    layer.fields = Object.assign({}, a.fields, b.fields);

    return layer;
}

function mergeInput(output, input, inputs, verbose, maxzoom, callback) {
    //Iterate tiles in input
    var sql = "SELECT zoom_level AS z, tile_column AS x, tile_row AS y FROM tiles";
    var params = [];
    if(maxzoom) {
        sql += " WHERE zoom_level <= ?";
        params.push(maxzoom);
    }
    input._db.all(sql, params, function(err, rows) {
        console.log("Processing " + rows.length + " rows from input");
        var bar;
        if(!verbose) {
         bar = new ProgressBar(':bar', { total: rows.length });
        }

        if(err) return callback(err);
        async.eachOfLimit(rows, CONCURRENT_TILES, function(row, index, eachCallback){
            var x = row.x;
            var z = row.z;
            var y = flipY(row.y, z);

            //check if tile exists in output
            output.tileExists(z, x, y, function(err, existing) {
                if(existing) { 
                    // console.log("Tile exists, skipping");
                    if(bar) {
                        bar.tick();
                    }
                    eachCallback();
                } else { //tile does not yet exist in output, so merge it
                    mergeTile(output, inputs, x, y, z, verbose, function(err) {
                        if(bar) {
                            bar.tick();
                        }
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

function mergeTile(output, inputs, x, y, z, verbose, callback) {
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
            if(verbose) {
                console.log("Using existing tile for " + z + "/" + x + "/" + y);
            }
            outputTile = tilesWithData[0];
        } else { // > 1
            outputTile = new mapnik.VectorTile(z, x, y);
            if(verbose) {
                console.log("Merging " + tilesWithData.length + " tiles to " + z + "/" + x + "/" + y);
            }
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
