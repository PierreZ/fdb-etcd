package fr.pierrezemb.fdb.layer.etcd.store;

import mvccpb.EtcdIoKvProto;

import java.util.List;

public class LatestOperations {
  public long readVersion;
  public List<EtcdIoKvProto.Event> events;

  public LatestOperations(long readVersion, List<EtcdIoKvProto.Event> events) {
    this.readVersion = readVersion;
    this.events = events;
  }
}


