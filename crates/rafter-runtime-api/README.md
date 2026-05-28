# rafter-runtime-api

`rafter-runtime-api` defines the small persist-before-output runtime contract
used by higher Rafter layers. It depends only on `rafter`; concrete durable
runtime, storage, service, and application crates implement or consume the
trait without making lower layers depend on those implementations.
