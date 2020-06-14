package fr.pierrezemb.fdb.layer.etcd.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;
import etcdserverpb.EtcdIoRpcProto;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchRecordStore {
  private static final Logger log = LoggerFactory.getLogger(WatchRecordStore.class);
  private final EtcdRecordMetadata recordLayer;

  public WatchRecordStore(EtcdRecordMetadata recordMeta) {
    this.recordLayer = recordMeta;
  }

  public void put(EtcdIoRpcProto.WatchCreateRequest createRequest) {


    log.debug("storing watch {}", createRequest);

    EtcdRecord.Watch record = EtcdRecord.Watch.newBuilder()
      .setKey(createRequest.getKey())
      .setRangeEnd(createRequest.getRangeEnd())
      .setWatchId(createRequest.getWatchId())
      .build();

    recordLayer.db.run(context -> {
      FDBRecordStore recordStore = recordLayer.recordStoreProvider.apply(context);
      recordStore.saveRecord(record);
      return null;
    });
  }

  public void delete(long watchId) {
    recordLayer.db.run(context -> {
      FDBRecordStore recordStore = recordLayer.recordStoreProvider.apply(context);
      recordStore.deleteRecord(Tuple.from(watchId));
      return null;
    });
  }
}
