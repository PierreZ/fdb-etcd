package fr.pierrezemb.fdb.layer.etcd;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {


  private GenericContainer fdb = new GenericContainer("foundationdb/foundationdb:6.2.19")
    .withExposedPorts(4500)
    .waitingFor(Wait.forListeningPort());
  private String clusterFilePath;
  // etcd client
  private KV kvClient;

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, ExecutionException, InterruptedException {
    fdb.start();
    fdb.getLogs();

    Thread.sleep(3 * 1000);

    //Container.ExecResult initResult = fdb.execInContainer("fdbcli", "--exec", "\"configure new single memory ; status\"");
    Container.ExecResult initResult = fdb.execInContainer("fdbcli", "--exec", "configure new single memory");
    String stdout = initResult.getStdout();
    System.out.println(stdout);
    int exitCode = initResult.getExitCode();
    System.out.println(exitCode);

    boolean fdbReady = false;

    // waiting for fdb to be up and healthy
    while (!fdbReady) {

      Container.ExecResult statusResult = fdb.execInContainer("fdbcli", "--exec", "status");
      stdout = statusResult.getStdout();

      if (stdout.contains("Healthy")) {
        fdbReady = true;
        System.out.println("fdb is healthy");
      } else {
        System.out.println("fdb is unhealthy");
        Thread.sleep(10 * 1000);
      }
    }

    // handle fdb cluster file
    Path path = Files.createTempDirectory("java-fdb-etcd-tests");
    clusterFilePath = path.toAbsolutePath().toString() + "/fdb.cluster";
    System.out.println(clusterFilePath);
    fdb.copyFileFromContainer("/var/fdb/fdb.cluster", clusterFilePath);


    // check record store
    EtcdRecordStore recordStore = new EtcdRecordStore(clusterFilePath);

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFilePath)
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
    // create client
    Client client = Client.builder().endpoints("http://localhost:8080").build();
    kvClient = client.getKVClient();

    ByteSequence key = ByteSequence.from("test_key".getBytes());
    ByteSequence value = ByteSequence.from("test_value".getBytes());

    // put the key-value
    kvClient.put(key, value).get();

    GetResponse response = kvClient.get(key).get();
    System.out.println(response.getKvs().get(0).getValue());

    testContext.completeNow();
  }

}
