package fr.pierrezemb.fdb.layer.etcd.service;

import fr.pierrezemb.fdb.layer.etcd.store.AuthRecordStore;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordMeta;
import fr.pierrezemb.fdb.layer.etcd.store.KVRecordStore;
import fr.pierrezemb.fdb.layer.etcd.store.LeaseRecordStore;

/**
 * RecordService is exposing all RecordStores in a easy way
 */
public class RecordService {
  public final KVRecordStore kv;
  public final LeaseRecordStore lease;
  public final AuthRecordStore auth;

  public RecordService(EtcdRecordMeta recordMeta) {
    this.kv = new KVRecordStore(recordMeta);
    this.lease = new LeaseRecordStore(recordMeta);
    this.auth = new AuthRecordStore(recordMeta);
  }
}
