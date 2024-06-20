# HTTP POST Ingest

Example:

```
Method:   POST
Url:      http://sf-ingest-mg-01.psi.ch:9009/daqingest/ingest/v1?channelName=MY:DEVICE:POS&shape=[]&scalarType=f32
Headers:  Content-Type: application/cbor-framed
```

The body must be a stream of length delimited frames, where the payload of each frame is
a CBOR object.

The http body of the response then looks like this:
```
[CBOR-frame]
[CBOR-frame]
[CBOR-frame]
... etc
```

where each `[CBOR-frame]` looks like:
```
[length N of the following CBOR object: uint32 little-endian]
[reserved: 12 bytes of zero-padding]
[CBOR object: N bytes]
[padding: P zero-bytes, 0 <= P <= 7, such that (N + P) mod 8 = 0]
```

Each CBOR object must contain the timestamps (integer nanoseconds) and the values (depends on type), e.g:
```json
{
  "tss": [1712100002000000000, 1712100003000000000, 1712100004000000000],
  "values": [5.6, 7.8, 8.1]
}
```

## Shape of data

The `shape` URL parameter indicates whether the data is scalar or 1-dimensional,
for example `shape=[]` indicates a scalar and `shape=[4096]` indicates an array
with 4096 elements.

The shape nowadays only distinguishes between scalar and 1-dimensional, but the actual length of
the array dimension may vary from event to event and is therefore not meaningful.
Still, it doesn't hurt to pass the "typical" size of array data as parameter.
