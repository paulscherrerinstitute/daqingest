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
api_bind: "0.0.0.0:3011"
# The hostname to send to channel access peers as our own hostname:
local_epics_hostname: sf-daqsync-02.psi.ch
# The backend name to use for the channels handled by this daqingest instance:
backend: scylla
# Hosts to use for channel access search:
search:
    - "172.26.0.255"
    - "172.26.2.255"
    - "172.26.8.255"
    - "..."
postgresql:
    host: postgresql-host
    port: 5432
    user: database-username
    pass: the-password
    name: the-database-name
scylla:
    hosts:
        - "sf-nube-11:19042"
        - "sf-nube-12:19042"
        - "sf-nube-13:19042"
        - "sf-nube-14:19042"
    keyspace: ks1
channels:
    - "SOME-CHANNEL:1"
    - "OTHER-CHANNEL:2"
```
