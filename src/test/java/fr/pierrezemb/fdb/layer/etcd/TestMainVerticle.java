package fr.pierrezemb.fdb.layer.etcd;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

//  private GenericContainer fdb = new GenericContainer("foundationdb/foundationdb:6.2.19")
//    .withExposedPorts(4500)
//    .waitingFor(Wait.forListeningPort());
//  private String clusterFilePath;
//
//  @BeforeEach
//  void setUp() throws IOException {
//    fdb.start();
//    fdb.getLogs();
//
//    Path path = Files.createTempDirectory("java-fdb-etcd-tests");
//    clusterFilePath = path.toAbsolutePath().toString() + "/fdb.cluster";
//    System.out.println(clusterFilePath);
//    fdb.copyFileFromContainer("/var/fdb/fdb.cluster", clusterFilePath);
//  }
//
//  @BeforeEach
//  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
//
//    vertx.deployVerticle(new MainVerticle(), testContext.succeeding(id -> testContext.completeNow()));
//  }
//
//  @Test
//  void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
//    testContext.completeNow();
//  }
}
