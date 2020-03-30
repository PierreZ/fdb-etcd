package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdRecordStore {
  private static Logger log = LoggerFactory.getLogger(EtcdRecordStore.class);

  public final FDBDatabase db;
  public final Function<FDBRecordContext, FDBRecordStore> recordStoreProvider;
  private final KeySpace keySpace;
  private final KeySpacePath path;

  public EtcdRecordStore(String clusterFilePath) {

    log.info("creating FDB Record store using cluster file @" + clusterFilePath);

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

    // This can be stored within FDB,
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStoreTest.java#L101
    //
    // Create a primary key composed of the key and version
    metadataBuilder.getRecordType("KeyValue").setPrimaryKey(Key.Expressions.concat(
      Key.Expressions.field("key"),
      Key.Expressions.field("version")
    ));

    // record-layer has the capacity to set versions on each records. Default is to use versionstamp
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/docs/Overview.md#indexing-by-version
    metadataBuilder.setStoreRecordVersions(true);

    recordStoreProvider = context -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metadataBuilder)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();
  }

  /**
   * get an Etcd record.
   *
   * @param key
   * @return
   */
  public EtcdRecord.KeyValue get(byte[] key) {
    log.trace("retrieving record {}", key);
    List<EtcdRecord.KeyValue> records = db.run(context -> {
      RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("key").equalsValue(key));

      RecordQuery query = queryBuilder.build();

      return runQuery(context, query);
    });

    if (records.size() == 0) {
      log.warn("cannot find any record for key {}", key);
      return null;
    }

    // return the highest version for a given key
    return records.stream().max(Comparator.comparing(EtcdRecord.KeyValue::getVersion)).get();
  }

  private List<EtcdRecord.KeyValue> runQuery(FDBRecordContext context, RecordQuery query) {
    // this returns an asynchronous cursor over the results of our query
    return recordStoreProvider
      .apply(context)
      // this returns an asynchronous cursor over the results of our query
      .executeQuery(query)
      .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(queriedRecord.getRecord()).build())
      .map(r -> {
        log.trace("found a record: {}", r);
        return r;
      })
      .asList().join();
  }

  public List<EtcdRecord.KeyValue> scan(byte[] start, byte[] end) {
    log.trace("scanning between {} and {}", start, end);
    return db.run(context -> {
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.and(
          Query.field("key").greaterThanOrEquals(start),
          Query.field("key").lessThanOrEquals(end)
        )).build();

      return runQuery(context, query);
    });
  }

  public void put(EtcdRecord.KeyValue record) {
    this.db.run(context -> {

      // First we need to retrieve the right revision
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("key").equalsValue(record.getKey())).build();

      long version = runQuery(context, query).stream().map(EtcdRecord.KeyValue::getVersion).max(Long::compare).orElse(0L);
      EtcdRecord.KeyValue fixedRecord = record.toBuilder().setVersion(version).build();

      log.trace("putting record {} in version {}", fixedRecord.toString(), version);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);
      recordStore.saveRecord(fixedRecord);
      log.trace("successfully put record {} in version {}", fixedRecord.toString(), version);
      return null;
    });
  }

  public Integer delete(byte[] start, byte[] end) {
    Integer count = this.db.run(context -> {
      log.trace("deleting between {} and {}", start, end);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);


      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.and(
          Query.field("key").greaterThanOrEquals(start),
          Query.field("key").lessThanOrEquals(end)
        )).build();

      return recordStoreProvider
        .apply(context)
        // this returns an asynchronous cursor over the results of our query
        .executeQuery(query)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .map(r -> {
          log.trace("found a record to delete: {}", r);
          return r;
        })
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), null)))
        .getCount()
        .join();
    });
    log.trace("deleted {} records", count);
    return count;
  }
}
