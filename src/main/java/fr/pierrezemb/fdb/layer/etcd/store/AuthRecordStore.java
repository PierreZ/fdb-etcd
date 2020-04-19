package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthRecordStore {
  private static final Logger log = LoggerFactory.getLogger(AuthRecordStore.class);

  private final EtcdRecordMeta etcdRecordMeta;

  public AuthRecordStore(EtcdRecordMeta etcdRecordMeta) {
    this.etcdRecordMeta = etcdRecordMeta;
  }

  public void roleAdd(EtcdRecord.Role role) {
    etcdRecordMeta.db.run(context -> {
      return this.etcdRecordMeta.recordStoreProvider.apply(context).saveRecord(role);
    });
  }

  public EtcdRecord.Role getRole(String role) {
    return etcdRecordMeta.db.run(context -> {

      FDBStoredRecord<Message> message = this.etcdRecordMeta.recordStoreProvider
        .apply(context)
        .loadRecord(Tuple.from(role));
      if (null == message) {
        return null;
      }

      return EtcdRecord.Role.newBuilder().mergeFrom(message.getRecord()).build();
    });
  }
}
