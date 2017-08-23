# Mortar
Combine MBTiles of Mapbox Vector Tiles.

Mortar was designed to combine multiple MBTiles produced by [TippeCanoe](https://github.com/mapbox/tippecanoe) into a single MBTiles file.

2 use cases for combining MBTiles were considered:
1. Source data split into arbitrary polygons, resulting in multiple tile sets that each have the same layer, and tiles on the edges of each set need to be merged.
2. Multiple data sets covering the same area, resulting in multiple tile sets that have different layers, but cover the same area.


