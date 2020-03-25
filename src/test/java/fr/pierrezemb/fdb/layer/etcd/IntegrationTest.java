package fr.pierrezemb.fdb.layer.etcd;

import static org.junit.Assert.assertEquals;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationTest extends FDBTestBase {

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

    GetResponse response = kvClient.get(key).get();
    assertEquals(1, response.getKvs().size());
    KeyValue put = response.getKvs().get(0);
    assertEquals(key, put.getKey());
    assertEquals(value, put.getValue());

    kvClient.delete(key);
    GetResponse responseAfterDelete = kvClient.get(key).get();
    assertEquals(0, responseAfterDelete.getKvs().size());

    testContext.completeNow();
  }

  @AfterAll
  void tearDown() {
    super.internalShutdown();
  }

}
