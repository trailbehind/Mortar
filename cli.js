#!/usr/bin/env node

function usage() {
  console.log("--output output.mbtiles 1.mbtiles 2.mbtiles");
}

var argv = require("minimist")(process.argv.slice(2), {
  output: "string",
  verbose: "boolean",
  maxzoom: "integer",
  concurrency: "integer"
});
var async = require("async");
var fs = require("fs");
var geojsonMerge = require("@mapbox/geojson-merge");
var mapnik = require("mapnik");
var mbtiles = require("@mapbox/mbtiles");
var ProgressBar = require("progress");
var rbush = require("rbush");
var Q = require("q");
var zlib = require("zlib");

mapnik.register_default_input_plugins();

mbtiles.prototype.tileExists = function(z, x, y, callback) {
  y = (1 << z) - 1 - y;
  // console.log("tileExists");

  var sql =
    "SELECT count(1) as count FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?";
  var mbtiles = this;
  var params = [z, x, y];
  this._db.get(sql, params, function(err, row) {
    var exists = row.count != 0;
    callback(err, exists);
  });
};

var outputPath = argv.output;
var verbose = argv.verbose;
var maxzoom = argv.maxzoom;
var CONCURRENT_TILES = argv.concurrency || 2;

if (maxzoom === undefined) {
  maxzoom = 24;
}
var inputs = argv._;

if (!outputPath || !inputs || !inputs.length) {
  usage();
  process.exit(1);
}

if (fs.existsSync(outputPath)) {
  console.log("Output path already exists");
  process.exit(1);
}

var inputMBTilesPath = [];
async.eachSeries(
  inputs,
  function(f, callback) {
    fs.stat(f, function(err, stats){
      if(err) {
        throw(err);
      } else if (stats.isFile()) {
        inputMBTilesPath.push(f);
        callback();
      } else if (stats.isDirectory()) {
        console.log("Reading direcotry");
        fs.readdir(f, function(err, items) {
          for (var i = 0; i < items.length; i++) {
            var itemPath = f + "/" + items[i];
            if (!itemPath.toLowerCase().endsWith(".mbtiles")) {
              continue;
            }
            var itemStats = fs.statSync(itemPath);
            if (itemStats.isFile()) {
              inputMBTilesPath.push(itemPath);
            }
          }
          callback();
        });
      } else {
        throw "Error, file is not a file or directory";
      }
    });
  },
  function(err) {
    if (err) throw err;
    if (inputMBTilesPath.length < 2) {
      usage();
      process.exit(1);
    }

    var inputTiles = [];
    console.log("opening " + inputMBTilesPath.length + " inputs");
    async.eachSeries(
      inputMBTilesPath,
      function(path, callback) {
        new mbtiles(path, function(err, tiles) {
          if (err) {
            console.log("Error opening input " + path);
            throw err;
          }
          inputTiles.push(tiles);
          callback();
        });
      },
      function(err) {
        if (err) throw err;
        console.log("opening output");

        new mbtiles(outputPath, function(err, tiles) {
          if (err) throw err;
          merge(tiles, inputTiles, verbose, maxzoom);
        });
      }
    );
  }
);

function merge(output, inputs, verbose, maxzoom) {
  console.log("Starting merge");
  async.waterfall(
    [
      function(callback) {
        console.log("Loading infos");
        async.each(
          inputs,
          function(input, eachCallback) {
            input.getInfo(function(err, info) {
              if (err) return eachCallback(err);
              input.info = info;
              input.minX = info.bounds[0];
              input.minY = info.bounds[1];
              input.maxX = info.bounds[2];
              input.maxY = info.bounds[3];
              eachCallback(null);
            });
          },
          function(err) {
            callback(err);
          }
        );
      },
      function(callback) {
        console.log("Merging infos");
        var info = inputs
          .map(function(input) {
            return input.info;
          })
          .reduce(function(a, b, i) {
            return mergeInfo(a, b, i);
          });
        callback(null, info);
      },
      function(info, callback) {
        console.log("writing info");
        output.startWriting(function(err) {
          if (err) return callback(err);
          output.putInfo(info, function(err) {
            if (err) return callback(err);
            output.stopWriting(function(err) {
              callback(err);
            });
          });
        });
      },
      function(callback) {
        console.log("merging inputs");
        var inputsIndex = rbush();
        inputsIndex.load(inputs);
        async.eachOfLimit(
          inputs,
          1,
          function(input, index, eachCallback) {
            console.log("merge input " + (index + 1) + "/" + inputs.length);
            output.startWriting(function(err) {
              if (err) return eachCallback(err);
              mergeInput(output, input, inputsIndex, verbose, maxzoom, function(err) {
                if (err) return eachCallback(err);
                inputsIndex.remove(input);
                console.log(
                  "Merging " + (index + 1) + "/" + inputs.length + " finished, calling stopWriting"
                );
                output.stopWriting(function(err) {
                  eachCallback(err);
                });
              });
            });
          },
          function(err) {
            if (err) console.log("error merging input", err);
            callback(err);
          }
        );
      }
    ],
    function(err) {
      console.log("done");
      if (err) {
        console.log(err);
        throw err;
      }
    }
  );
}

function mergeInfo(a, b) {
  var info = {};
  if (a.attribution || b.attribution) {
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
  info.center = [
    (info.bounds[0] + info.bounds[2]) / 2,
    (info.bounds[1] + info.bounds[3]) / 2,
    Math.round((info.maxzoom + info.minzoom) / 2)
  ];
  info.format = a.format;
  info.version = a.version;
  info.name = [a.name, b.name].join(" + ");

  var vectorLayers = {};
  [a, b].forEach(function(source) {
    if (source.vector_layers) {
      source.vector_layers.forEach(function(l) {
        if (vectorLayers[l.id]) {
          vectorLayers[l.id] = mergeLayerDefinition(vectorLayers[l.id], l);
        } else {
          vectorLayers[l.id] = l;
        }
      });
    }
  });

  info.vector_layers = Object.keys(vectorLayers).map(function(k) {
    return vectorLayers[k];
  });
  info.id = [a.id, b.id].join(",");

  return info;
}

function mergeLayerDefinition(a, b) {
  var layer = {
    id: a.id
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
  if (maxzoom !== undefined) {
    sql += " WHERE zoom_level <= ?";
    params.push(maxzoom);
  }
  var overlappingInputs = inputs.search(input);
  input._db.all(sql, params, function(err, rows) {
    console.log(
      "Processing " +
        rows.length +
        " rows from input, seaching " +
        overlappingInputs.length +
        " sources for tiles"
    );
    var bar;
    if (!verbose) {
      bar = new ProgressBar(":elapseds :percent :bar :current/:total", {
        total: rows.length
      });
    }

    if (err) return callback(err);
    async.eachOfLimit(
      rows,
      CONCURRENT_TILES,
      function(row, index, eachCallback) {
        var x = row.x;
        var z = row.z;
        var y = flipY(row.y, z);

        //check if tile exists in output
        output.tileExists(z, x, y, function(err, existing) {
          if (existing) {
            // console.log("Tile exists, skipping");
            if (bar) {
              bar.tick();
            }
            eachCallback();
          } else {
            //tile does not yet exist in output, so merge it
            mergeTile(output, overlappingInputs, x, y, z, verbose, function(err) {
              if (bar) {
                bar.tick();
              }
              eachCallback(err);
            });
          }
        });
      },
      function(err) {
        if (err) {
          console.log("Error iterating tiles: ", err);
        }
        callback(err);
      }
    );
  });
}

function mergeTile(output, inputs, x, y, z, verbose, callback) {
  //Fetch tile from each input
  var promises = [];
  inputs.forEach(function(input) {
    var deferred = Q.defer();
    promises.push(deferred.promise);
    input.getTile(z, x, y, function(err, data) {
      if (data) {
        try {
          var inputTile = new mapnik.VectorTile(z, x, y);
          inputTile.setDataSync(data);
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
  Q.all(promises).then(
    function(tiles) {
      var tilesWithData = [];
      for (var i = 0; i < tiles.length; i++) {
        if (tiles[i]) {
          tilesWithData.push(tiles[i]);
        }
      }

      if (tilesWithData.length == 1) {
        if (verbose) {
          console.log("Using existing tile for " + z + "/" + x + "/" + y);
        }
        var data = tilesWithData[0].getData({
          compression: "gzip",
          level: 9
        });
        output.putTile(z, x, y, data, function(err) {
          callback(err);
        });
      } else {
        // > 1
        if (verbose) {
          console.log("Merging " + tilesWithData.length + " tiles to " + z + "/" + x + "/" + y);
        }
        var layers = {};
        async.eachLimit(
          tilesWithData,
          1,
          function(tile, tileCallback) {
            async.eachLimit(
              tile.names(),
              1,
              function(layerName, layerCallback) {
                if (layers[layerName]) {
                  try {
                    mergeTileLayers(layerName, layers[layerName], tile, function(err, mergedLayer) {
                      if (err) {
                        return layerCallback(err);
                      }
                      layers[layerName] = mergedLayer;
                      layerCallback(err);
                    });
                  } catch (err) {
                    layerCallback(err);
                  }
                } else {
                  layers[layerName] = tile.layer(layerName);
                  layerCallback();
                }
              },
              function(err) {
                tileCallback(err);
              }
            );
          },
          function(err) {
            if (err) return callback(err);
            //write to output
            var layersList = Object.keys(layers).map(function(k) {
              return layers[k];
            });
            var outputTile = new mapnik.VectorTile(z, x, y);
            outputTile.compositeSync(layersList);
            var data = outputTile.getData({
              compression: "gzip",
              level: 9
            });
            output.putTile(z, x, y, data, function(err) {
              callback(err);
            });
          }
        );
      }
    },
    function(err) {
      if (err) console.log(err);
      callback(err);
    }
  );
}

function mergeTileLayers(layerName, a, b, callback) {
  try {
    var datas = [];
    try {
      datas.push(JSON.parse(a.toGeoJSONSync(layerName)));
    } catch (err) {
      console.log("Error in mergeTileLayers" + err);
    }
    try {
      datas.push(JSON.parse(b.toGeoJSONSync(layerName)));
    } catch (err) {
      console.log("Error in mergeTileLayers" + err);
    }
    var mergedData = geojsonMerge.merge(datas);
    var newTile = new mapnik.VectorTile(a.z, a.x, a.y);
    newTile.tileSize = a.tileSize;
    newTile.bufferSize = a.bufferSize;
    newTile.addGeoJSON(JSON.stringify(mergedData), layerName);
    callback(null, newTile);
  } catch (err) {
    console.log("Error in mergeTileLayers", err);
    callback(err, null);
  }
}

function flipY(y, z) {
  return (1 << z) - 1 - y;
}
