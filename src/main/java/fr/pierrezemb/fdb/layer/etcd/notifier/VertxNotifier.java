package fr.pierrezemb.fdb.layer.etcd.notifier;

import com.google.protobuf.InvalidProtocolBufferException;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import mvccpb.EtcdIoKvProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Notifier that use Vertx Event Bus
 */
public class VertxNotifier implements Notifier {
  private static final Logger log = LoggerFactory.getLogger(VertxNotifier.class);
  private final EventBus eventBus;

  public VertxNotifier(EventBus eventBus) {
    this.eventBus = eventBus;
  }

  @Override
  public void publish(String tenant, long watchID, EtcdIoKvProto.Event event) {
    log.info("publishing to {}", tenant + watchID);
    this.eventBus.publish(tenant + watchID, event.toByteArray());
  }

  @Override
  public void watch(String tenant, long watchID, Handler<EtcdIoKvProto.Event> handler) {
    log.info("listening on {}", tenant + watchID);
    this.eventBus.consumer(tenant + watchID, message -> {
      log.info("received a message from the eventbus: '{}'", message);
      if (message.body() instanceof byte[]) {
        try {
          EtcdIoKvProto.Event event = EtcdIoKvProto.Event.newBuilder().mergeFrom((byte[]) message.body()).build();
          handler.handle(event);
        } catch (InvalidProtocolBufferException e) {
          log.error("cannot create Event: '{}', skipping", e.toString());
        }
      } else {
        log.error("received a message wich is not byte[], skipping");
      }
    });
  }
}
