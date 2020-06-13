package fr.pierrezemb.fdb.layer.etcd.notifier;

import com.google.protobuf.Message;

/**
 * Notifier are used to pass watched keys between KVService and WatchService
 * Standalone version can use the Vertx Event bus implementation,
 * but we should add Kafka/Pulsar implementation for distributed mode
 */
public interface Notifier {
  void publish(String tenant, long watchID, Message message);
  void watch(String tenant, long watchID);
}
