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

mbtiles.prototype.tileExists = function(z, x, y, callback) {
    y = (1 << z) - 1 - y;

    var sql = 'SELECT count(1) as count FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?';
    var mbtiles = this;
    this._db.get(sql, z, x, y, function(err, row) {
        callback(null, row.count != 0);
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
    async.waterfall([function(callback){
        console.log("startWriting");
        output.startWriting(function(err){
            if(err) return callback(err);
            callback(null);
        });
    },
    function(callback) {
        console.log("copying info");
        inputs[0].getInfo(function(err, info){
            if(err) return callback(err);
            output.putInfo(info, function(err){
                callback(err);
            });
        });
    },
    function(callback){
        console.log("merging inputs");
        async.eachOfLimit(inputs, 1, function(input, index, eachCallback){
            console.log("merge input " + (index + 1) + "/" + inputs.length);
            mergeInput(output, input, inputs, eachCallback);
        }, function(err) {
            if(err) console.log("error merging input", err);
            callback(err);
        });
    },
    function(callback){
        console.log("stopWriting");
        output.stopWriting(function(err) {
            if (err) return callback(err);
            console.log("Finished");
        });
    }], function(err){
        console.log("done");
        if(err){
            console.log(err);
            throw err;
        }
    });
}

function mergeInput(output, input, inputs, callback) {
    //Iterate tiles in input
    var promises = [];
    input._db.each("SELECT zoom_level AS z, tile_column AS x, tile_row AS y FROM tiles", function(err, row) {
        var promise = Q.defer();
        promises.push(promise.promise);

        if(err) return promise.reject(err);
        var x = row.x;
        var z = row.z;
        var y = flipY(row.y, z);

        //check if tile exists in output
        output.tileExists(z, x, y, function(err, existing) {
            if(existing) { 
                console.log("Tile exists, skipping");
                promise.resolve();
            } else { //tile does not yet exist in output, so merge it
                mergeTile(output, inputs, x, y, z, function(err) {
                    if(err) {
                        promise.reject(err);
                    } else {
                        promise.resolve();
                    }
                });
            }
        });
    }, function(err, rows){
        if(err) return callback(err);
        console.log("Input completed with " + rows + " rows. " + promises.length + " promises.");
        Q.allSettled(promises).then(function(results) {
            callback(null);
        }, function(err) {
            callback(err);
        });
    });
}

function mergeTile(output, inputs, x, y, z, callback) {
//    console.log("mergeing tile " + z + "/" + x + "/" + y);

    //Fetch tile from each input
    var promises = [];
    inputs.forEach(function(input){
        var deferred = Q.defer();
        promises.push(deferred);
        input.getTile(z, x, y, function(err, data) {
            return deferred.reject(err);
            var inputTile = new mapnik.VectorTile(z, x, y);
            inputTile.setData(data);
            deferred.resolve(inputTile);
        });
    });

    //Merge all the input tiles
    Q.all(promises).then(function(tiles) {
        console.log("Merging " + tiles.length + " tiles to " + z + "/" + x + "/" + y);
        var outputTile = new mapnik.VectorTile(z, x, y);
        try {
            outputTile.composite(tiles);
        } catch (err) {
            callback(err);
        }

        //write to output
        zlib.gzip(outputTile.getData(), function(err, data) {
            output.write(z, x, y, data, function(err) {
                callback(err);
            });
        });
    }, function(err) {
        callback(err);
    });
}

function flipY(y, z) {
    return (1 << z) - 1 - y;
}
