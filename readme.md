# DAQ Ingest

## Build

```
cargo build --release
```

The resulting binary is found at `target/release/daqingest` and dynamically links only
to the most basic linux system libraries.


## Run

```
./daqingest channel-access ca-ingest <CONFIG.YML>
```


## Config file example

```yml
# Address to bind the HTTP API to, for runtime control and Prometheus metrics scrape:
api_bind: 0.0.0.0:3011
# The backend name to use for the channels handled by this daqingest instance:
backend: scylla
channels: directory-name-with-channel-config-files
# Addresses to use for channel access search:
search:
    - "172.26.0.255"
    - "172.26.2.255"
    - "172.26.8.255"
    - "..."
postgresql:
    host: postgresql-host
    port: 5432
    user: the-username
    pass: the-password
    name: the-database
scylla_st:
    keyspace: backend_st
    hosts:
        - sf-nube-11:19042
        - sf-nube-12:19042
        - sf-nube-13:19042
        - sf-nube-14:19042
scylla_mt:
    keyspace: backend_mt
    hosts:
        - sf-nube-11:19042
        - sf-nube-12:19042
        - sf-nube-13:19042
        - sf-nube-14:19042
scylla_lt:
    keyspace: backend_lt
    hosts:
        - sf-nube-11:19042
        - sf-nube-12:19042
        - sf-nube-13:19042
        - sf-nube-14:19042
```


## Access status and configuration of daqingest at runtime

Status and configuration can be accessed at runtime via http at the address
as configured by the `api_bind` parameter.

### Check the state of a channel

```txt
http://<api_bind>/daqingest/channel/state?name=[...]
```


# HTTP POST ingest

It is possible to [ingest](postingest.md) data via the `api_bind` socket address.
