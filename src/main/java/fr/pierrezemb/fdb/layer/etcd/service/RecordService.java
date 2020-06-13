package fr.pierrezemb.fdb.layer.etcd.service;

import fr.pierrezemb.fdb.layer.etcd.recordlayer.EtcdRecordMetadata;
import fr.pierrezemb.fdb.layer.etcd.recordlayer.KVRecordStore;
import fr.pierrezemb.fdb.layer.etcd.recordlayer.LeaseRecordStore;
import fr.pierrezemb.fdb.layer.etcd.recordlayer.WatchRecordStore;

/**
 * RecordService is exposing all RecordStores in a easy way
 */
public class RecordService {
  public final KVRecordStore kv;
  public final LeaseRecordStore lease;
  public final WatchRecordStore watch;

  public RecordService(EtcdRecordMetadata recordMeta) {
    this.kv = new KVRecordStore(recordMeta);
    this.lease = new LeaseRecordStore(recordMeta);
    this.watch = new WatchRecordStore(recordMeta);
  }
}
