package fr.pierrezemb.fdb.layer.etcd.notifier;

import com.google.protobuf.Message;

public interface Notifier {
  void publish(String tenant, long watchID, Message message);
  void watch(String tenant, long watchID);
}
