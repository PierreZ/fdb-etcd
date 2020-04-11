package fr.pierrezemb.fdb.layer.etcd;

import fr.pierrezemb.fdb.layer.etcd.service.KVService;
import fr.pierrezemb.fdb.layer.etcd.service.LeaseService;
import fr.pierrezemb.fdb.layer.etcd.service.RecordService;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordMeta;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString("fdb-cluster-file");
    System.out.println("connecting to fdb@" + clusterFilePath);

    EtcdRecordMeta recordMeta = new EtcdRecordMeta(clusterFilePath);
    RecordService recordService = new RecordService(recordMeta);

    VertxServer server = VertxServerBuilder
      .forAddress(vertx, "localhost", 8080)
      .addService(new KVService(recordService))
      .addService(new LeaseService(recordService))
      .build();

    server.start(ar -> {
      if (ar.succeeded()) {
        System.out.println("gRPC service started");
        startPromise.complete();
      } else {
        System.out.println("Could not start server " + ar.cause().getMessage());
        startPromise.fail(ar.cause());
      }
    });
  }
}
