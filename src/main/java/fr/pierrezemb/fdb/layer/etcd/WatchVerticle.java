package fr.pierrezemb.fdb.layer.etcd;

import com.google.protobuf.AbstractMessageLite;
import fr.pierrezemb.fdb.layer.etcd.grpc.WatchService;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import fr.pierrezemb.fdb.layer.etcd.store.LatestOperations;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class WatchVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(WatchVerticle.class);

  private final String tenantId;
  private final long watchID;
  private final EtcdRecordLayer recordLayer;
  private long timerID;
  private long lastCommitedVersion;
  private final Semaphore mutex;

  public WatchVerticle(String tenantId, long watchId, EtcdRecordLayer recordLayer, long commitVersion) {
    this.tenantId = tenantId;
    this.watchID = watchId;
    this.recordLayer = recordLayer;
    this.lastCommitedVersion = commitVersion;
    this.mutex = new Semaphore(1);

  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    log.trace("starting timer");
    this.timerID = vertx.setTimer(10, id -> {
      this.poll();
    });
  }

  private void poll() {
    long elapsed = 100;
    try {
      mutex.acquire();
      log.trace("poll");
      long start = System.currentTimeMillis();
      LatestOperations ops = this.recordLayer.retrieveLatestOperations(tenantId, lastCommitedVersion);
      elapsed = System.currentTimeMillis() - start;
      log.trace("poll finished: found {} events in {}ms", ops.events.size(), elapsed);
      this.lastCommitedVersion = ops.readVersion;
      ops.events.stream()
        .map(AbstractMessageLite::toByteArray)
        .forEach(e -> this.vertx.eventBus().publish(tenantId + watchID, e));
      log.trace("old readVersion: {}, new readVersion: {}", lastCommitedVersion, ops.readVersion);
      this.lastCommitedVersion = ops.readVersion;
    } catch (InterruptedException e) {
      // exception handling code
    } finally {
      mutex.release();
    }

    log.trace("scheduling a poll in {}ms", 2 * elapsed);
    vertx.setTimer(2 * elapsed, timerID -> poll());
  }

}
