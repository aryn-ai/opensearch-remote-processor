# Remote Search Processor Plugin
This plugin adds a remote search processor that calls out to the sycamore search processing service.

## Instructions for use with remote-processor-service

 -> assumes you came from [remote-processor-service](https://github.com/aryn-ai/remote-processor-service) instructions

This part should be fairly simple. I think gradle handles all of the code-gen, so all that should be needed is to checkout 2.x and get the proto files
```
git switch 2.x
git submodule update --init --remote
```
and then build a zip with
```
./gradlew assemble
```
the zip is at `build/distributions/remote-processor-2.12.0-SNAPSHOT.zip`

## License
This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright
Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
