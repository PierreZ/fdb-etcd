package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EtcdRecordStore {

  // Keep a global track of the number of records stored
  protected static final Index COUNT_INDEX = new Index(
    "globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);

  // keep track of the version per key with an index
  protected static final Index INDEX_VERSION_PER_KEY = new Index(
    "index-version-per-key",
    Key.Expressions.field("version").groupBy(Key.Expressions.field("key")),
    IndexTypes.MAX_EVER_LONG);

  private static final Logger log = LoggerFactory.getLogger(EtcdRecordStore.class);
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

    // record-layer has the capacity to set versions on each records.
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/docs/Overview.md#indexing-by-version
    metadataBuilder.setStoreRecordVersions(true);

    // This can be stored within FDB,
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStoreTest.java#L101
    //
    // Create a primary key composed of the key and version
    metadataBuilder.getRecordType("KeyValue").setPrimaryKey(Key.Expressions.concat(
      Key.Expressions.field("key"),
      Key.Expressions.field("version"))); // Version in primary key not supported, so we need to use our version

    // create index on `mod_revision` field, which is used to implement fetch of revisions
    // not working: 	Suppressed: java.util.concurrent.ExecutionException:
    // com.apple.foundationdb.record.metadata.expressions.KeyExpression$InvalidExpressionException:
    // there must be exactly 1 version entry in version index
    // metadataBuilder.addIndex("KeyValue",
    //   new Index("kv-version", Key.Expressions.field("mod_revision"), IndexTypes.VERSION));

    metadataBuilder.addIndex("KeyValue", new Index(
      "keyvalue-modversion", Key.Expressions.field("mod_revision"), IndexTypes.VALUE));

    // add an index to easily retrieve the max version for a given key, instead of scanning
    metadataBuilder.addIndex("KeyValue", INDEX_VERSION_PER_KEY);

    // add a global index that will count all records and updates
    metadataBuilder.addUniversalIndex(COUNT_INDEX);

    recordStoreProvider = context -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metadataBuilder)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();
  }

  public EtcdRecord.KeyValue get(byte[] toByteArray) {
    return get(toByteArray, 0);
  }

  /**
   * get an Etcd record.
   *
   * @param key
   * @return
   */
  public EtcdRecord.KeyValue get(byte[] key, long revision) {
    log.trace("retrieving record {} for revision {}", Arrays.toString(key), revision);
    List<EtcdRecord.KeyValue> records = db.run(context -> {
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          revision == 0 ?
            // no revision
            Query.field("key").equalsValue(key) :
            // with revision
            Query.and(
              Query.field("key").equalsValue(key),
              Query.field("mod_revision").lessThanOrEquals(revision))
        ).build();

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
      .asList().join();
  }

  public List<EtcdRecord.KeyValue> scan(byte[] start, byte[] end) {
    return scan(start, end, 0);
  }

  public List<EtcdRecord.KeyValue> scan(byte[] start, byte[] end, long revision) {
    log.trace("scanning between {} and {} with revision {}", start, end, revision);
    return db.run(context -> {
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          revision == 0 ?
            Query.and(
              Query.field("key").greaterThanOrEquals(start),
              Query.field("key").lessThanOrEquals(end)) :
            Query.and(
              Query.and(
                Query.field("key").greaterThanOrEquals(start),
                Query.field("key").lessThanOrEquals(end)),
              Query.field("mod_revision").lessThanOrEquals(revision))
        ).build();

      return runQuery(context, query);
    });

  }

  public EtcdRecord.KeyValue put(EtcdRecord.KeyValue record) {
    return this.db.run(context -> {

      // retrieve version using an index
      IndexAggregateFunction function = new IndexAggregateFunction(
        FunctionNames.MAX_EVER, INDEX_VERSION_PER_KEY.getRootExpression(), INDEX_VERSION_PER_KEY.getName());
      Tuple maxResult = recordStoreProvider.apply(context)
        .evaluateAggregateFunction(
          Collections.singletonList("KeyValue"), function,
          Key.Evaluated.concatenate(record.getKey().toByteArray()), IsolationLevel.SERIALIZABLE)
        .join();

      if (log.isTraceEnabled()) {
        // debug: let's scan and print the index to see how it is built:
        recordStoreProvider.apply(context).scanIndex(
          INDEX_VERSION_PER_KEY,
          IndexScanType.BY_GROUP,
          TupleRange.ALL,
          null, // continuation,
          ScanProperties.FORWARD_SCAN
        ).asList().join().stream().forEach(
          indexEntry -> log.trace("found an indexEntry: key:'{}', value: '{}'", indexEntry.getKey(), indexEntry.getValue())
        );
      }


      long version = null == maxResult ? 1 : maxResult.getLong(0) + 1;

      EtcdRecord.KeyValue fixedRecord = record.toBuilder()
        .setVersion(version)
        .setCreateRevision(context.getReadVersion())
        .setModRevision(context.getReadVersion()) // using read version as mod revision
        .build();

      log.trace("putting record key '{}' in version {}", fixedRecord.getKey().toStringUtf8(), version);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);
      recordStore.saveRecord(fixedRecord);
      log.trace("successfully put record {} in version {}", fixedRecord.toString(), fixedRecord.getVersion());
      return fixedRecord;
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
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), record.getVersion())))
        .getCount()
        .join();
    });
    log.trace("deleted {} records", count);
    return count;
  }

  public void compact(long revision) {
    Integer count = this.db.run(context -> {
      log.warn("compacting any record before {}", revision);
      FDBRecordStore recordStore = recordStoreProvider.apply(context);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("mod_revision").lessThanOrEquals(revision)).build();

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
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), record.getVersion())))
        .getCount()
        .join();
    });
    log.trace("deleted {} records", count);
  }

  /**
   * Compute stats using Indexes
   * A (secondary) index in the Record Layer is a subspace of the record store uniquely associated with the index.
   * This subspace is updated when records are inserted or updated in the same transaction
   * so that it is always consistent with the (primary) record data.
   * <p>
   * Here we have a global (above any record store) index that is updated.
   * We can then retrieve for example number of records
   *
   * @return
   */
  public long stats() {
    Long stat = this.db.run(context -> {
      IndexAggregateFunction function = new IndexAggregateFunction(
        FunctionNames.COUNT, COUNT_INDEX.getRootExpression(), COUNT_INDEX.getName());
      FDBRecordStore recordStore = recordStoreProvider.apply(context);

      if (log.isTraceEnabled()) {
        // debug: let's scan and print the index to see how it is built:
        recordStoreProvider.apply(context).scanIndex(
          COUNT_INDEX,
          IndexScanType.BY_GROUP,
          TupleRange.ALL,
          null, // continuation,
          ScanProperties.FORWARD_SCAN
        ).asList().join().stream().forEach(
          indexEntry -> log.trace("found an indexEntry for stats: key:'{}', value: '{}'", indexEntry.getKey(), indexEntry.getValue())
        );
      }

      return recordStore.evaluateAggregateFunction(EvaluationContext.EMPTY, Collections.singletonList("KeyValue"), function, TupleRange.ALL, IsolationLevel.SERIALIZABLE)
        .thenApply(tuple -> tuple.getLong(0));
    }).join();

    log.info("we have around {} ETCD records", stat);

    return stat;
  }
}
