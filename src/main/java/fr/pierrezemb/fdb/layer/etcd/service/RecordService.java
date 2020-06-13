package fr.pierrezemb.fdb.layer.etcd.service;

import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordMeta;
import fr.pierrezemb.fdb.layer.etcd.store.KVRecordStore;
import fr.pierrezemb.fdb.layer.etcd.store.LeaseRecordStore;
import fr.pierrezemb.fdb.layer.etcd.store.WatchRecordStore;

/**
 * RecordService is exposing all RecordStores in a easy way
 */
public class RecordService {
  public final KVRecordStore kv;
  public final LeaseRecordStore lease;
  public final WatchRecordStore watch;

  public RecordService(EtcdRecordMeta recordMeta) {
    this.kv = new KVRecordStore(recordMeta);
    this.lease = new LeaseRecordStore(recordMeta);
    this.watch = new WatchRecordStore(recordMeta);
  }
}
