package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
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
import java.util.Arrays;
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
      Key.Expressions.field("version")));

    // create ranking index on version
    metadataBuilder.addIndex("KeyValue",
      new Index("version_rank_per_key", Key.Expressions.field("version").groupBy(Key.Expressions.field("key")), IndexTypes.RANK));

    // record-layer has the capacity to set versions on each records.
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/docs/Overview.md#indexing-by-version
    metadataBuilder.setStoreRecordVersions(true);

    recordStoreProvider = context -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metadataBuilder)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();
  }

  public Result get(byte[] toByteArray) {
    return get(toByteArray, 0, 0);
  }

  /**
   * get an Etcd record.
   *
   * @param key
   * @return
   */
  public Result get(byte[] key, long version, long revision) {
    log.trace("retrieving record {} for version {}", Arrays.toString(key), version);
    Result records = db.run(context -> {


      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          version == 0 ?
            // if version is zero, we need to retrieve latest
            Query.field("key").equalsValue(key) :
            // if version is superior to zero, we need to retrieve the exact version
            // because 0 is the null value in protobuf 3, versions needs to start at 1
            Query.and(
              Query.field("key").equalsValue(key),
              Query.field("version").equalsValue(version))
        ).build();

      return runQuery(context, query, revision);
    });

    if (records.records.size() == 0) {
      log.warn("cannot find any record for key {}", key);
      return null;
    }

    // return the highest version for a given key
    EtcdRecord.KeyValue results = records.records.stream().max(Comparator.comparing(EtcdRecord.KeyValue::getVersion)).get();
    return new Result(Arrays.asList(results), records.readVersion);
  }

  private Result runQuery(FDBRecordContext context, RecordQuery query, long revision) {
    if (revision > 0) {
      log.trace("setting readVersion to " + revision);
      context.setReadVersion(revision);
    }

    log.trace("running query " + context.getReadVersion());
    // this returns an asynchronous cursor over the results of our query
    List<EtcdRecord.KeyValue> records = recordStoreProvider
      .apply(context)
      // this returns an asynchronous cursor over the results of our query
      .executeQuery(query)
      .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(queriedRecord.getRecord()).build())
      .map(r -> {
        log.trace("found a record: " + r.toString());
        return r;
      })
      .asList().join();
    return new Result(records, context.getReadVersion());
  }

  public Result scan(byte[] start, byte[] end, long version, long revision) {
    log.trace("scanning between {} and {} with version {}", start, end, version);
    return db.run(context -> {
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          version == 0 ?
            Query.and(
              Query.field("key").greaterThanOrEquals(start),
              Query.field("key").lessThanOrEquals(end)) :
            Query.and(
              Query.and(
                Query.field("key").greaterThanOrEquals(start),
                Query.field("key").lessThanOrEquals(end)),
              Query.field("version").equalsValue(version))
        ).build();

      return runQuery(context, query, revision);
    });
  }

  public Result put(EtcdRecord.KeyValue record) {
    return this.db.run(context -> {

      // First we need to retrieve the right revision
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("key").equalsValue(record.getKey().toByteArray())).build();

      long version = runQuery(context, query, 0).records.stream()
        .map(EtcdRecord.KeyValue::getVersion)
        .max(Long::compare).orElse(0L);
      version += 1;
      EtcdRecord.KeyValue fixedRecord = record.toBuilder()
        .setVersion(version) // set version for the given key, starting at 1
        .setModRevision(context.getReadVersion()) // setting revision using fdb readVersion
        .build();

      log.trace("putting record key '{}' in version {}", fixedRecord.getKey().toStringUtf8(), version);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);
      recordStore.saveRecord(fixedRecord);
      log.trace("successfully put record {} in version {}", fixedRecord.toString(), fixedRecord.getVersion());
      return new Result(Arrays.asList(fixedRecord), context.getReadVersion());
    });
  }

  public DeleteResult delete(byte[] start, byte[] end) {

    return this.db.run(context -> {
      log.trace("deleting between {} and {}", start, end);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.and(
          Query.field("key").greaterThanOrEquals(start),
          Query.field("key").lessThanOrEquals(end)
        )).build();

      Integer count = recordStoreProvider
        .apply(context)
        // this returns an asynchronous cursor over the results of our query
        .executeQuery(query)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .map(r -> {
          log.trace("found a record to delete: {}", r);
          return r;
        })
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), record.getVersion())))
        .getCount()
        .join();
      log.trace("deleted {} records", count);
      return new DeleteResult(context.getReadVersion(), count.intValue());
    });
  }

  public Result get(byte[] toByteArray, int revision) {
    return get(toByteArray, 0, revision);
  }

  public Result scan(byte[] toByteArray, byte[] toByteArray1, int revision) {
    return scan(toByteArray, toByteArray1, 0, revision);
  }

  public Result scan(byte[] bytes, byte[] bytes1) {
    return scan(bytes, bytes1, 0, 0);
  }

  public class Result {
    private final List<EtcdRecord.KeyValue> records;
    private final long readVersion;

    public Result(List<EtcdRecord.KeyValue> records, long readVersion) {
      this.records = records;
      this.readVersion = readVersion;
    }

    public List<EtcdRecord.KeyValue> getRecords() {
      return records;
    }

    public long getReadVersion() {
      return readVersion;
    }
  }

  public class DeleteResult {
    private final long readVersion;
    private final int count;

    public DeleteResult(long readVersion, int count) {
      this.readVersion = readVersion;
      this.count = count;
    }

    public long getCount() {
      return count;
    }

    public long getReadVersion() {
      return readVersion;
    }
  }


}
