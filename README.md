# fdb-etcd ![https://img.shields.io/badge/vert.x-3.8.5-purple.svg](https://img.shields.io/badge/vert.x-3.8.5-purple.svg) ![gradle build](https://github.com/PierreZ/fdb-etcd/workflows/gradle%20build/badge.svg)

An experiment to provide ETCD layer on top of FoundationDB, built with [Record-Layer](https://foundationdb.github.io/fdb-record-layer/) and [Vert.x](https://vertx.io/).

## Features

* etcd protobuf was imported and exposed with Vert.x,
* Record-layer is used. [As etcd is also using protobuf, we are directly storing the PutRequest](https://github.com/PierreZ/fdb-etcd/blob/master/src/main/proto/record.proto),
* Integrations test using a real FDB spawned with testcontainers and official Java etcd client,
* Supported operations:
    * put
    * get
    * scan
    * delete

For TODO's, please have a look to the [Github issues](https://github.com/pierrez/fdb-etcd/issues).

## Building

### Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2

### Gradle cheat-sheet

To launch your tests:
```
./gradlew clean test
```

To package your application:
```
./gradlew clean assemble
```

To run your application:
```
./gradlew clean run
```

## Contributing

Pull requests are very welcome. I will try to keep as [Github issues](https://github.com/pierrez/fdb-etcd/issues) what needs to be done if you want to jump in!

For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Resources

* [https://vertx.io/docs/vertx-grpc/java/](https://vertx.io/docs/vertx-grpc/java/)
* [https://foundationdb.github.io/fdb-record-layer/GettingStarted.html](https://foundationdb.github.io/fdb-record-layer/GettingStarted.html)
