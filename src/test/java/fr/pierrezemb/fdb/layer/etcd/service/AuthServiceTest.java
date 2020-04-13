package fr.pierrezemb.fdb.layer.etcd;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.PutResponse;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuthServiceTest {

  private static final ByteSequence SAMPLE_KEY = ByteSequence.from("sample_key".getBytes());
  private static final ByteSequence SAMPLE_VALUE = ByteSequence.from("sample_value".getBytes());
  private static final ByteSequence SAMPLE_KEY_2 = ByteSequence.from("sample_key2".getBytes());
  private static final ByteSequence SAMPLE_VALUE_2 = ByteSequence.from("sample_value2".getBytes());
  private static final ByteSequence SAMPLE_KEY_3 = ByteSequence.from("sample_key3".getBytes());

  private FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath())
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void testAuth() throws Exception {
    final Client rootClient = Client.builder()
      .endpoints("http://localhost:8080")
      .user(ByteSequence.from("root".getBytes()))
      .password(ByteSequence.from("roopasswd".getBytes())).build();

    KV kvClient = rootClient.getKVClient();
    CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY, SAMPLE_VALUE);
    PutResponse response = feature.get();
    assertNotNull(response.getHeader());
    assertFalse(response.hasPrevKv());
  }
}
