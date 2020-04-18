package fr.pierrezemb.fdb.layer.etcd.service;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EtcdTenant is creating RecordService with the right tenant
 */
public class RecordServiceBuilder {
  private static final Logger log = LoggerFactory.getLogger(RecordServiceBuilder.class);
  private final String clusterFilePath;
  private final FDBDatabase db;

  public RecordServiceBuilder(String clusterFilePath) {
    this.clusterFilePath = clusterFilePath;
    // get DB
    log.info("creating FDB Record store using cluster file @" + clusterFilePath);
    db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);
  }

  public RecordService withTenant(String tenant) {
    log.info("using tenant {}", tenant);
    return new RecordService(new EtcdRecordMeta(db, tenant));
  }
}
