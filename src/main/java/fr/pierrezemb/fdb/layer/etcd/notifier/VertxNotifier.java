package fr.pierrezemb.fdb.layer.etcd.notifier;

import com.google.protobuf.Message;
import io.vertx.core.eventbus.EventBus;

public class VertxNotifier implements Notifier {
  private final EventBus eventBus;

  public VertxNotifier(EventBus eventBus) {
    this.eventBus = eventBus;
  }

  @Override
  public void publish(String tenant, long watchID, Message message) {

  }

  @Override
  public void watch(String tenant, long watchID) {

  }
}
