package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;
import etcdserverpb.EtcdIoRpcProto;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;

public class WatchRecordStore {
  private final EtcdRecordMeta recordLayer;
    public WatchRecordStore(EtcdRecordMeta recordMeta) {
      this.recordLayer = recordMeta;
    }

  public void put(EtcdIoRpcProto.WatchCreateRequest createRequest) {

    EtcdRecord.Watch record = EtcdRecord.Watch.newBuilder()
      .setKey(createRequest.getKey())
      .setRangeEnd(createRequest.getRangeEnd())
      .setWatchId(createRequest.getWatchId())
      .build();

    recordLayer.db.run(context -> {
      FDBRecordStore recordStore = recordLayer.recordStoreProvider.apply(context);
      recordStore.saveRecord(record);
      context.commit();
      return null;
    });
  }

  public void delete(long watchId) {
      recordLayer.db.run(context -> {
        FDBRecordStore recordStore = recordLayer.recordStoreProvider.apply(context);
        recordStore.deleteRecord(Tuple.from(watchId));
        context.commit();
        return null;
      });
  }
}
