package fr.pierrezemb.fdb.layer.etcd.store;

import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseRecordStore {
  private static final Logger log = LoggerFactory.getLogger(LeaseRecordStore.class);
  private final EtcdRecordMeta recordLayer;

  public LeaseRecordStore(EtcdRecordMeta recordLayer) {
    this.recordLayer = recordLayer;
  }

  public void put(EtcdRecord.Lease lease) {
    log.debug("putting lease {}", lease.toString());
    recordLayer.db.run(fdbRecordContext ->
      this.recordLayer.recordStoreProvider.apply(fdbRecordContext).saveRecord(lease));
  }
}
