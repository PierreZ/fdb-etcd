package fr.pierrezemb.fdb.layer.etcd;

import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordStore;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMainVerticle extends FDBTestBase {

  private KV kvClient;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    super.internalSetup();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFilePath)
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void integrationCRUD(Vertx vertx, VertxTestContext testContext) throws Throwable {
    // create client
    Client client = Client.builder().endpoints("http://localhost:8080").build();
    kvClient = client.getKVClient();

    ByteSequence key = ByteSequence.from("test_key".getBytes());
    ByteSequence value = ByteSequence.from("test_value".getBytes());

    // put the key-value
    kvClient.put(key, value).get();

    // GetResponse response = kvClient.get(key).get();
    // System.out.println(response.getKvs().get(0).getValue());

    testContext.completeNow();
  }

  @AfterAll
  void tearDown() {
    super.internalShutdown();
  }

}
