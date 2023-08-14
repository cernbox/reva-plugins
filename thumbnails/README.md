# Thumbnails plugin

The Thumbnails service is an HTTP plugin for reva that generates thumbnails from images stored in reva. This service provides a convenient way to generate and retrieve thumbnails for various image formats (png, jpeg and bmp).

## Features

- Automatically generates thumbnails for various image formats.
- Efficiently caches and retrieves thumbnails to enhance performance.

## Configuration

```
[http.services.thumbnails]
quality = 80
fixed_resolutions = ["16x16", "32x32"]
cache = "lru"
output_type = "jpg"
prefix = "thumbnails"
insecure = false
```

`quality`: jpeg quality (0-100). Used only if `output_type` is set to `jpg` (default is 80).
`fixed_resolution`: a list of fixed resolution to generate. Each resolution is of type `<widht>x<height>`.
`cache`: cache driver to use. Possible values are `""` (no cache), `lru` (use and in memory lru cache strategy).
`output_type`: output type of the generated thumbnails. Possible values are `bmp`, `png` and `jpg`.
`prefix`: prefix where the http service will listen (default to `thumbnails`).

To configure the cache:

```
[http.services.thumbnails.cache_drivers.lru]
size = 1000
expiration = 300
```

`size`: cache size (default is 1000000).
`expiration`: expiration in second of the cached entries (default to 300).