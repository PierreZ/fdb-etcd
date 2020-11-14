package fr.pierrezemb.fdb.layer.etcd;

import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
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
  private final QueryComponent keyQueryFilter;
  private final Semaphore mutex;
  private long timerID;
  private long lastCommitedVersion;

  public WatchVerticle(String tenantId, long watchId, EtcdRecordLayer recordLayer, long commitVersion, ByteString rangeStart, ByteString rangeEnd) {
    this.tenantId = tenantId;
    this.watchID = watchId;
    this.recordLayer = recordLayer;
    this.lastCommitedVersion = commitVersion;
    this.mutex = new Semaphore(1);

    this.keyQueryFilter = rangeEnd.size() == 0 ?
      Query.field("key").equalsValue(rangeStart.toByteArray()) :
      Query.and(
        Query.field("key").greaterThanOrEquals(rangeStart.toByteArray()),
        Query.field("key").lessThanOrEquals(rangeEnd.toByteArray())
      );
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    log.trace("starting timer");
    this.timerID = vertx.setTimer(10, id -> {
      this.poll();
    });
  }

  private void poll() {
    long nextPollMS = 500;
    try {
      mutex.acquire();
      log.trace("poll started");
      long start = System.currentTimeMillis();
      LatestOperations ops = this.recordLayer.retrieveLatestOperations(tenantId, lastCommitedVersion, keyQueryFilter);
      long elapsed = System.currentTimeMillis() - start;
      log.trace("poll finished: found {} events in {}ms", ops.events.size(), elapsed);

      if (elapsed > 100 && elapsed < 1000) {
        nextPollMS = elapsed * 2;
      }

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

    log.trace("scheduling a poll in {}ms", nextPollMS);
    vertx.setTimer(nextPollMS, timerID -> poll());
  }

}
