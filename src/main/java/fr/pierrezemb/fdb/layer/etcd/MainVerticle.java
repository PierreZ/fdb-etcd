package fr.pierrezemb.fdb.layer.etcd;

import fr.pierrezemb.fdb.layer.etcd.grpc.AuthInterceptor;
import fr.pierrezemb.fdb.layer.etcd.grpc.AuthService;
import fr.pierrezemb.fdb.layer.etcd.grpc.KVService;
import fr.pierrezemb.fdb.layer.etcd.grpc.LeaseService;
import fr.pierrezemb.fdb.layer.etcd.service.RecordServiceBuilder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString("fdb-cluster-file", "/var/fdb/fdb.cluster");
    boolean authEnabled = this.context.config().getBoolean("auth-enabled", false);
    String defaultTenant = this.context.config().getString("default-tenant", "default");
    System.out.println("connecting to fdb@" + clusterFilePath);

    RecordServiceBuilder recordServiceBuilder = new RecordServiceBuilder(clusterFilePath);

    VertxServerBuilder serverBuilder = VertxServerBuilder
      .forAddress(vertx,
        this.context.config().getString("listen-address", "localhost"),
        this.context.config().getInteger("listen-port", 8080))
      .intercept(new AuthInterceptor(authEnabled, defaultTenant))
      .addService(new KVService(recordServiceBuilder))
      .addService(new LeaseService(recordServiceBuilder))
      .addService(new AuthService());

    VertxServer server = serverBuilder.build();

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
