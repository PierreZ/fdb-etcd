package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import java.util.List;
import java.util.function.Function;

public class EtcdRecordStore {
  public final FDBDatabase db;
  public final Function<FDBRecordContext, FDBRecordStore> recordStoreProvider;
  private final KeySpace keySpace;
  private final KeySpacePath path;

  public EtcdRecordStore(String clusterFilePath) {

    // get DB
    db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);

    // In this case, the key space implies that there are multiple "applications"
    // that might be defined to run on the same FoundationDB cluster, and then
    // each "application" might have multiple "environments". This could be used,
    // for example, to connect to either the "prod" or "qa" environment for the same
    // application from within the same code base.
    keySpace = new KeySpace(
      new DirectoryLayerDirectory("application")
        .addSubdirectory(new KeySpaceDirectory("user", KeySpaceDirectory.KeyType.STRING))
    );

    // Create a path for the "etcd-store" application's and "user1" user
    path = keySpace.path("application", "etcd-store").add("user", "user1");

    RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
      .setRecords(EtcdRecord.getDescriptor());

    metadataBuilder.getRecordType("KeyValue").setPrimaryKey(Key.Expressions.field("key"));
    // This can be stored within FDB,
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStoreTest.java#L101

    recordStoreProvider = context -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metadataBuilder)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();
  }

  public EtcdRecord.KeyValue get(Tuple key) {
    return db.run(context -> {
      FDBStoredRecord<Message> storedMessage = recordStoreProvider.apply(context).loadRecord(key);
      if (storedMessage == null) {
        return null;
      }
      return EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(storedMessage.getRecord())
        .build();
    });
  }

  public List<EtcdRecord.KeyValue> scan(Tuple start, Tuple end) {
    return db.run(context -> {
      FDBRecordStore recordStore = recordStoreProvider.apply(context);

      // this returns an asynchronous cursor over the results of our query
      return recordStore.scanRecords(
        start, end,
        EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
        null, ScanProperties.FORWARD_SCAN)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .asList().join();
    });
  }

  public void put(EtcdRecord.KeyValue record) {
    this.db.run(context -> {
      FDBRecordStore recordStore = recordStoreProvider.apply(context);
      return recordStore.saveRecord(record);
    });
  }

  public Integer delete(Tuple start, Tuple end) {
    return this.db.run(context -> {
      FDBRecordStore recordStore = recordStoreProvider.apply(context);
      // TODO: split code and return list of deletes keys

      return recordStore.scanRecords(
        start, end,
        EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
        null, ScanProperties.FORWARD_SCAN)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder().mergeFrom(queriedRecord.getRecord()).build())
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray())))
        .getCount()
        .join();
    });
  }
}
