[package]
name = "mithril-proto"
version = "0.1.0"
edition = "2024"

[features]
default = []

[dependencies]
bytes = "1.1.0"
prost = "0.14.3"
pbjson = "0.7"
pbjson-types = "0.7"
serde = "1.0"
tonic = { version = "0.14.5", features = ["gzip"] }
tonic-prost = "0.14.5"
