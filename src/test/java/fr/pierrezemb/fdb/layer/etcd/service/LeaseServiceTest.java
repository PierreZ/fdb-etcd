package fr.pierrezemb.fdb.layer.etcd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import fr.pierrezemb.fdb.layer.etcd.MainVerticle;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Observers;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
    kvClient.put(KEY_2, VALUE, PutOption.newBuilder().withLeaseId(leaseID).build()).get();
    assertEquals(1, kvClient.get(KEY_2).get().getCount());
    leaseClient.revoke(leaseID).get();
    assertEquals(0, kvClient.get(KEY_2).get().getCount());
  }

  @Test
  public void testKeepAliveOnce() throws InterruptedException, ExecutionException {
    long leaseID = leaseClient.grant(2).get().getID();
    kvClient.put(KEY, VALUE, PutOption.newBuilder().withLeaseId(leaseID).build()).get();
    assertEquals(1, kvClient.get(KEY).get().getCount());
    LeaseKeepAliveResponse rp = leaseClient.keepAliveOnce(leaseID).get();
    assertTrue(rp.getTTL() > 0);
  }

  @Test
  public void testKeepAlive() throws ExecutionException, InterruptedException {
    long leaseID = leaseClient.grant(2).get().getID();
    kvClient.put(KEY, VALUE, PutOption.newBuilder().withLeaseId(leaseID).build()).get();
    assertEquals(1, kvClient.get(KEY).get().getCount());

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<LeaseKeepAliveResponse> responseRef = new AtomicReference<>();
    StreamObserver<LeaseKeepAliveResponse> observer = Observers.observer(response -> {
      responseRef.set(response);
      latch.countDown();
    });

    try (CloseableClient c = leaseClient.keepAlive(leaseID, observer)) {
      latch.await(5, TimeUnit.SECONDS);
      LeaseKeepAliveResponse response = responseRef.get();
      assertTrue(response.getTTL() > 0);
    }

    Thread.sleep(3000);
    assertEquals(0, kvClient.get(KEY).get().getCount());
  }
}
