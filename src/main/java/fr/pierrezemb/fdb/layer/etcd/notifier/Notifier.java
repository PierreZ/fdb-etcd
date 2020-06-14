package fr.pierrezemb.fdb.layer.etcd.notifier;

import com.google.protobuf.Message;
import io.vertx.core.Handler;
import mvccpb.EtcdIoKvProto;

/**
 * Notifier are used to pass watched keys between KVService and WatchService
 * Standalone version can use the Vertx Event bus implementation,
 * but we should add Kafka/Pulsar implementation for distributed mode
 */
public interface Notifier {
  void publish(String tenant, long watchID, EtcdIoKvProto.Event event);
  // TODO: abstract the event bus so that it is a true interface
  void watch(String tenant, long watchID, Handler<EtcdIoKvProto.Event> handler);
}
