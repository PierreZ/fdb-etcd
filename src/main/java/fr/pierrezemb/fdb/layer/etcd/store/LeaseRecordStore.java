package fr.pierrezemb.fdb.layer.etcd.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseRecordStore {
  private static final Logger log = LoggerFactory.getLogger(LeaseRecordStore.class);
  private final EtcdRecordMeta recordLayer;

  public LeaseRecordStore(EtcdRecordMeta recordLayer) {
    this.recordLayer = recordLayer;
  }

  public void putLease(long id, long ttl, long currentTimeMillis) {

  }
}
