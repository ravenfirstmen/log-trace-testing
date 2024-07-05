#!/bin/bash

# don't bother with s3cr3t
nats --server="nats://s3cr3t@localhost:4222" pub create '{"key":"key1", "info":"my-info"}' -H version:V1

# com parent tracer
parent_trace=$(uuidgen -r -x | tr -d '-')
parent_span=$(uuidgen -r -x | tr -d '-' | cut -b -16)
full_trace="00-${parent_trace}-${parent_span}-01"
echo "Sending trace: ${full_trace}"
# don't bother with s3cr3t
nats --server="nats://s3cr3t@localhost:4222" pub create '{"key":"key2", "info":"my-info-sem-trace"}' -H version:V1 -H traceparent:"${full_trace}"

