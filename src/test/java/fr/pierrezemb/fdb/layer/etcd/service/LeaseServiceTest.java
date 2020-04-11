package fr.pierrezemb.fdb.layer.etcd.service;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import fr.pierrezemb.fdb.layer.etcd.MainVerticle;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.options.PutOption;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Taken from https://github.com/etcd-io/jetcd/blob/master/jetcd-core/src/test/java/io/etcd/jetcd/LeaseTest.java
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LeaseServiceTest {
  private static final ByteSequence KEY = ByteSequence.from("foo", Charsets.UTF_8);
  private static final ByteSequence KEY_2 = ByteSequence.from("foo2", Charsets.UTF_8);
  private static final ByteSequence VALUE = ByteSequence.from("bar", Charsets.UTF_8);
  private final FoundationDBContainer container = new FoundationDBContainer();
  private KV kvClient;
  private Client client;
  private File clusterFile;
  private Lease leaseClient;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath())
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));

    // create client
    client = Client.builder().endpoints("http://localhost:8080").build();
    // uncomment this to test on real etcd
    // client = Client.builder().endpoints("http://localhost:2379").build();
    kvClient = client.getKVClient();
    leaseClient = client.getLeaseClient();
  }

  @Test
  public void testGrant() throws Exception {
    // ttl is 5s
    long leaseID = leaseClient.grant(5).get().getID();

    kvClient.put(KEY, VALUE, PutOption.newBuilder().withLeaseId(leaseID).build()).get();
    assertEquals(kvClient.get(KEY).get().getCount(), 1);

    // let's wait 6s
    Thread.sleep(6000);
    assertEquals(kvClient.get(KEY).get().getCount(), 0);
  }

  @Test
  public void testRevoke() throws Exception {
    long leaseID = leaseClient.grant(5).get().getID();
    kvClient.put(KEY, VALUE, PutOption.newBuilder().withLeaseId(leaseID).build()).get();
    assertEquals(kvClient.get(KEY).get().getCount(), 1);
    leaseClient.revoke(leaseID).get();
    assertEquals(kvClient.get(KEY).get().getCount(), 0);
  }
}
