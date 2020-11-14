package fr.pierrezemb.fdb.layer.etcd.service;

import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import fr.pierrezemb.fdb.layer.etcd.MainVerticle;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent.EventType;
import io.etcd.jetcd.watch.WatchResponse;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static fr.pierrezemb.fdb.layer.etcd.TestUtil.randomByteSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@ExtendWith(VertxExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WatchServiceTest {
  private final FoundationDBContainer container = new FoundationDBContainer();
  private Client client;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    File clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath())
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    client = Client.builder().endpoints("http://localhost:8080").build();

  }

  @Test
  @Order(1)
  public void testWatchOnPut() throws Exception {
    final ByteSequence key = randomByteSequence();
    final CountDownLatch latch = new CountDownLatch(1);
    final ByteSequence value = randomByteSequence();
    final AtomicReference<WatchResponse> ref = new AtomicReference<>();

    try (Watcher watcher = client.getWatchClient().watch(key, response -> {
      ref.set(response);
      latch.countDown();
    })) {

      Thread.sleep(2000);

      client.getKVClient().put(key, value).get();
      latch.await(4, TimeUnit.SECONDS);

      watcher.close();

      assertNotNull(ref.get());
      assertEquals(1, ref.get().getEvents().size());
      assertEquals(EventType.PUT, ref.get().getEvents().get(0).getEventType());
      assertEquals(key, ref.get().getEvents().get(0).getKeyValue().getKey());
    }
  }

  @Test
  @Order(2)
  public void testWatchRangeOnPut() throws Exception {
    final ByteSequence key = ByteSequence.from("b", Charset.defaultCharset());
    final CountDownLatch latch = new CountDownLatch(1);
    final ByteSequence value = randomByteSequence();
    final AtomicReference<WatchResponse> ref = new AtomicReference<>();

    try (Watcher watcher = client.getWatchClient().watch(ByteSequence.from("a", Charset.defaultCharset()), WatchOption.newBuilder()
      .withRange(ByteSequence.from("c", Charset.defaultCharset()))
      .build(), response -> {
      ref.set(response);
      latch.countDown();
    })) {

      Thread.sleep(1000);

      client.getKVClient().put(key, value).get();
      latch.await(4, TimeUnit.SECONDS);

      watcher.close();

      assertNotNull(ref.get());
      assertEquals(1, ref.get().getEvents().size());
      assertEquals(EventType.PUT, ref.get().getEvents().get(0).getEventType());
      assertEquals(key, ref.get().getEvents().get(0).getKeyValue().getKey());
    }
  }

  @Test
  @Order(3)
  public void testWatchRangeOnDelete() throws Exception {
    final ByteSequence key = ByteSequence.from("g", Charset.defaultCharset());
    final ByteSequence value = randomByteSequence();
    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicReference<WatchResponse> ref = new AtomicReference<>();

    client.getKVClient().put(key, value).get();

    try (Watcher watcher = client.getWatchClient().watch(ByteSequence.from("f", Charset.defaultCharset()), WatchOption.newBuilder()
      .withRange(ByteSequence.from("h", Charset.defaultCharset()))
      .build(), response -> {
      ref.set(response);
      latch.countDown();
    })) {

      Thread.sleep(1000);

      System.out.println("start deleting");
      client.getKVClient().delete(key).get();
      System.out.println("deleted");
      latch.await(4, TimeUnit.SECONDS);
      System.out.println("checking");
      watcher.close();

      assertNotNull(ref.get());
      assertEquals(1, ref.get().getEvents().size());
      assertEquals(EventType.DELETE, ref.get().getEvents().get(0).getEventType());
      assertEquals(key, ref.get().getEvents().get(0).getKeyValue().getKey());
    }
  }
}
