package fr.pierrezemb.fdb.layer.etcd.store;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
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
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import etcdserverpb.EtcdIoRpcProto;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import mvccpb.EtcdIoKvProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.apple.foundationdb.record.TupleRange.ALL;

public class EtcdRecordLayer {

  // Keep a global track of the number of records stored
  protected static final Index COUNT_INDEX = new Index(
    "globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
  // keep track of the version per key with an index
  protected static final Index INDEX_VERSION_PER_KEY = new Index(
    "index-version-per-key",
    Key.Expressions.field("version").groupBy(Key.Expressions.field("key")),
    IndexTypes.MAX_EVER_LONG);

  protected static final Index VERSIONSTAMP_INDEX = new Index("kv-globalVersion", VersionKeyExpression.VERSION, IndexTypes.VERSION);

  private static final Logger LOGGER = LoggerFactory.getLogger(EtcdRecordLayer.class);
  private final FDBDatabase db;
  private final KeySpace keySpace = new KeySpace(
    new DirectoryLayerDirectory("application")
      .addSubdirectory(new KeySpaceDirectory("tenant", KeySpaceDirectory.KeyType.STRING))
  );


  public EtcdRecordLayer(String clusterFilePath) throws InterruptedException, ExecutionException, TimeoutException {
    db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);
    db.performNoOpAsync().get(2, TimeUnit.SECONDS);
  }

  public EtcdRecord.KeyValue get(String tenant, byte[] key) {
    return get(tenant, key, 0);
  }

  /**
   * get an Etcd record.
   *
   * @param key
   * @return
   */
  public EtcdRecord.KeyValue get(String tenantID, byte[] key, long revision) {
    LOGGER.trace("retrieving record {} for revision {}", Arrays.toString(key), revision);
    List<EtcdRecord.KeyValue> records = db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      return runQuery(fdbRecordStore, context, createGetQuery(key, revision));
    });

    if (records.size() == 0) {
      LOGGER.warn("cannot find any record for key {}", key);
      return null;
    }
    LOGGER.trace("found {} records for key {}", records.size(), key);

    // return the highest version for a given key
    return records.stream().max(Comparator.comparing(EtcdRecord.KeyValue::getVersion)).get();
  }

  private RecordQuery createGetQuery(byte[] key, long revision) {
    // creating the query
    return RecordQuery.newBuilder()
      .setRecordType("KeyValue")
      .setFilter(
        revision == 0 ?
          // no revision
          Query.and(
            Query.field("key").equalsValue(key),
            Query.field("is_deleted").isNull()
          ) :
          // with revision
          Query.and(
            Query.field("key").equalsValue(key),
            Query.field("is_deleted").isNull(),
            Query.field("mod_revision").lessThanOrEquals(revision))
      ).build();
  }

  public List<EtcdRecord.KeyValue> scan(String tenant, byte[] start, byte[] end) {
    return scan(tenant, start, end, 0);
  }

  public List<EtcdRecord.KeyValue> scan(String tenantID, byte[] start, byte[] end, long revision) {
    LOGGER.trace("scanning between {} and {} with revision {}", start, end, revision);
    return db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          revision == 0 ?
            Query.and(
              Query.field("key").greaterThanOrEquals(start),
              Query.field("key").lessThanOrEquals(end),
              Query.field("is_deleted").isNull()) :
            Query.and(
              Query.and(
                Query.field("key").greaterThanOrEquals(start),
                Query.field("key").lessThanOrEquals(end)),
              Query.field("is_deleted").isNull(),
              Query.field("mod_revision").lessThanOrEquals(revision))
        ).build();

      return runQuery(fdbRecordStore, context, query);
    });
  }


  public EtcdRecord.KeyValue put(String tenantID, EtcdRecord.KeyValue record) {
    return db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);

      // checking if we have a previous version of the key
      long version = 1;
      long createRevision = context.getReadVersion();
      List<EtcdRecord.KeyValue> previousVersions = runQuery(fdbRecordStore, context, createGetQuery(record.getKey().toByteArray(), 0));
      if (previousVersions.size() == 1) {
        EtcdRecord.KeyValue previousVersion = previousVersions.get(0);
        version = previousVersion.getVersion() + 1;
        createRevision = previousVersion.getCreateRevision();
        LOGGER.trace("found an old version {} for key {}, flagging it for compaction", previousVersion.getVersion(), previousVersion.getKey());
        fdbRecordStore.saveRecord(previousVersion.toBuilder().setRemoveDuringCompaction(true).build());
      }

      EtcdRecord.KeyValue fixedRecord = record.toBuilder()
        .setVersion(version)
        .setCreateRevision(createRevision)
        .setModRevision(context.getReadVersion()) // using read version as mod revision
        .setIsDeleted(false)
        .build();

      fdbRecordStore.saveRecord(fixedRecord);
      LOGGER.trace("successfully put record {}", fixedRecord.toString());

      return fixedRecord;
    });
  }

  private RecordQuery createWatchQuery(long revision) {
    return RecordQuery
      .newBuilder()
      .setRecordType("KeyValue")
      .setFilter(
        Query.version().greaterThan(revision)
      ).build();
  }

  public Integer delete(String tenantID, byte[] start, byte[] end) {
    Integer count = this.db.run(context -> {
      LOGGER.trace("deleting between {} and {}", start, end);
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.and(
          Query.field("key").greaterThanOrEquals(start),
          Query.field("key").lessThanOrEquals(end)
        )).build();

      return
        fdbRecordStore.executeQuery(query)
          .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
            .mergeFrom(queriedRecord.getRecord()).build())
          .map(r -> {
            LOGGER.trace("found a record to delete: {}", r);
            return r;
          })
          // delete records
          .map(record -> {
            record = record.toBuilder().setIsDeleted(true).setModRevision(context.getReadVersion()).build();
            fdbRecordStore.saveRecord(record);
            return record;
          })
          .getCount()
          .join();
    });
    LOGGER.trace("deleted {} records", count);
    return count;
  }

  public void compact(String tenantID, long revision) {
    Integer count = db.run(context -> {
      LOGGER.warn("compacting any record before {}", revision);
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          Query.or(
            Query.and(
              Query.field("remove_during_compaction").equalsValue(true),
              Query.field("mod_revision").lessThanOrEquals(revision)
            ),
            Query.field("is_deleted").equalsValue(true)
          )).build();

      return fdbRecordStore
        // this returns an asynchronous cursor over the results of our query
        .executeQuery(query)
        .map(r -> {
          LOGGER.trace("found a record to compact: {}", r.getPrimaryKey());
          return r;
        })
        .map(record -> fdbRecordStore.deleteRecord(record.getPrimaryKey()))
        .getCount()
        .join();
    });
    LOGGER.trace("deleted {} records", count);
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
  public long stats(String tenantID) {
    Long stat = db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      IndexAggregateFunction function = new IndexAggregateFunction(
        FunctionNames.COUNT, COUNT_INDEX.getRootExpression(), COUNT_INDEX.getName());

      return fdbRecordStore.evaluateAggregateFunction(EvaluationContext.EMPTY, Collections.singletonList("KeyValue"), function, ALL, IsolationLevel.SERIALIZABLE)
        .thenApply(tuple -> tuple.getLong(0));
    }).join();

    LOGGER.info("we have around {} ETCD records", stat);
    return stat;
  }


  public void put(String tenantID, EtcdRecord.Lease lease) {
    LOGGER.debug("putting lease {}", lease.toString());
    db.run(context -> createFDBRecordStore(context, tenantID).saveRecord(lease));
  }

  public void deleteLease(String tenantID, long leaseID) {
    db.run(context -> createFDBRecordStore(context, tenantID).deleteRecord(Tuple.from(leaseID)));
  }

  public EtcdRecord.Lease keepAlive(String tenantID, long leaseID) {
    return db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      FDBStoredRecord<Message> record = fdbRecordStore.loadRecord(Tuple.from(leaseID));
      if (record == null) {
        LOGGER.warn("lease {} cannot be found, exiting keepAlive", leaseID);
        return null;
      }
      EtcdRecord.Lease lease = EtcdRecord.Lease.newBuilder().mergeFrom(record.getRecord()).build();
      lease = EtcdRecord.Lease.newBuilder()
        .setInsertTimestamp(System.currentTimeMillis())
        .setTTL(lease.getTTL())
        .setID(leaseID)
        .build();

      fdbRecordStore.saveRecord(lease);
      return lease;
    });
  }

  public EtcdRecord.Lease get(String tenantID, long leaseID) {
    return db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      FDBStoredRecord<Message> record = fdbRecordStore.loadRecord(Tuple.from(leaseID));
      if (record == null) {
        LOGGER.warn("lease {} cannot be found, exiting keepAlive", leaseID);
        return null;
      }
      return EtcdRecord.Lease.newBuilder().mergeFrom(record.getRecord()).build();
    });
  }

  public List<EtcdRecord.KeyValue> getWithLease(String tenantID, long id) {
    LOGGER.trace("retrieving record for revision {}", id);
    return db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantID);
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          Query.field("lease").equalsValue(id)
        ).build();
      return runQuery(fdbRecordStore, context, query);
    });
  }

  public void deleteRecordsWithLease(String tenantId, long leaseID) {
    LOGGER.info("deleting all records with lease '{}'", leaseID);
    Integer count = db.run(context -> {
      FDBRecordStore fdbRecordStore = createFDBRecordStore(context, tenantId);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("lease").equalsValue(leaseID)).build();

      return fdbRecordStore
        // this returns an asynchronous cursor over the results of our query
        .executeQuery(query)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .map(r -> {
          LOGGER.trace("found a record to delete for lease {}: {}", r, leaseID);
          return r;
        })
        .map(record -> fdbRecordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), record.getVersion())))
        .getCount()
        .join();
    });
    LOGGER.trace("deleted {} records with lease {}", count, leaseID);
  }

  public long put(String tenantID, EtcdIoRpcProto.WatchCreateRequest watchCreateRequest) {
    LOGGER.debug("storing watch {}", watchCreateRequest);

    EtcdRecord.Watch record = EtcdRecord.Watch.newBuilder()
      .setKey(watchCreateRequest.getKey())
      .setRangeEnd(watchCreateRequest.getRangeEnd())
      .setWatchId(watchCreateRequest.getWatchId())
      .build();

    return db.run(context -> {
      FDBRecordStore recordStore = createFDBRecordStore(context, tenantID);
      recordStore.saveRecord(record);
      return context.getReadVersion();
    });
  }

  public void deleteWatch(String tenantID, long watchId) {
    db.run(context -> {
      FDBRecordStore recordStore = createFDBRecordStore(context, tenantID);
      recordStore.deleteRecord(Tuple.from(watchId));
      return null;
    });
  }

  public LatestOperations retrieveLatestOperations(String tenantId, long lastCommitedVersion) {
    AtomicLong readVersion = new AtomicLong();
    List<EtcdIoKvProto.Event> events = db.run(context -> {
      FDBRecordStore recordStore = createFDBRecordStore(context, tenantId);

      readVersion.set(context.getReadVersion());
      return runWatchQuery(recordStore, createWatchQuery(lastCommitedVersion));
    });

    return new LatestOperations(readVersion.get(), events);
  }


  private List<EtcdIoKvProto.Event> runWatchQuery(FDBRecordStore fdbRecordStore, RecordQuery query) {

    long now = System.currentTimeMillis();
    // this returns an asynchronous cursor over the results of our query
    return fdbRecordStore
      // this returns an asynchronous cursor over the results of our query
      .executeQuery(query, null, ExecuteProperties.newBuilder().setReturnedRowLimit(1000).build())
      .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(queriedRecord.getRecord()).build())
      // filter according to the lease
      .filter(record -> filterRecordWithExpiredLease(now, record, fdbRecordStore))
      .map(record -> {
        LOGGER.trace("found a record to forward to Watcher: {}", record);
        EtcdIoKvProto.Event.EventType eventType = record.getIsDeleted() ? EtcdIoKvProto.Event.EventType.DELETE : EtcdIoKvProto.Event.EventType.PUT;
        try {
          return EtcdIoKvProto.Event.newBuilder()
            .setType(eventType)
            .setKv(EtcdIoKvProto.KeyValue.newBuilder().mergeFrom(record.toByteArray()).build()).build();
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
          return null;
        }
      })
      .filter(Objects::nonNull)
      .asList().join();
  }

  private Boolean filterRecordWithExpiredLease(long now, EtcdRecord.KeyValue record, FDBRecordStore fdbRecordStore) {
    if (record.getLease() == 0) {
      // no lease
      return true;
    }
    // the record has a lease, retrieve it
    FDBStoredRecord<Message> leaseMsg = fdbRecordStore
      .loadRecord(Tuple.from(record.getLease()));
    if (leaseMsg == null) {
      LOGGER.debug("record '{}' has a lease '{}' that can not be found, filtering",
        record.getKey().toStringUtf8(), record.getLease());
      return false;
    }
    EtcdRecord.Lease lease = EtcdRecord.Lease.newBuilder().mergeFrom(leaseMsg.getRecord()).build();
    if (now > lease.getInsertTimestamp() + lease.getTTL() * 1000) {
      LOGGER.debug("record '{}' has a lease '{}' that has expired, filtering",
        record.getKey().toStringUtf8(), record.getLease());
      return false;
    }
    return true;
  }

  private List<EtcdRecord.KeyValue> runQuery(FDBRecordStore fdbRecordStore, FDBRecordContext context, RecordQuery query) {

    long now = System.currentTimeMillis();
    // this returns an asynchronous cursor over the results of our query
    return fdbRecordStore
      // this returns an asynchronous cursor over the results of our query
      .executeQuery(query)
      .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(queriedRecord.getRecord()).build())
      // filter according to the lease
      .filter(record -> filterRecordWithExpiredLease(now, record, fdbRecordStore))
      .asList().join();
  }

  public FDBRecordStore createFDBRecordStore(FDBRecordContext context, String tenant) {

    // Create a path for the "etcd-store" application's and "tenant" user
    KeySpacePath path = keySpace.path("application", "fdb-etcd").add("tenant", tenant);

    RecordMetaData recordMetada = createEtcdRecordMetadata();

    // Helper func
    Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(recordMetada)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();

    return recordStoreProvider.apply(context);
  }

  private RecordMetaData createEtcdRecordMetadata() {

    RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
      .setRecords(EtcdRecord.getDescriptor());

    setupKeyValue(metadataBuilder);
    setupLease(metadataBuilder);
    setupWatch(metadataBuilder);

    // record-layer has the capacity to set versions on each records.
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/docs/Overview.md#indexing-by-version
    metadataBuilder.setStoreRecordVersions(true);
    return metadataBuilder.build();
  }

  private void setupWatch(RecordMetaDataBuilder metadataBuilder) {
    metadataBuilder.getRecordType("Watch").setPrimaryKey(
      com.apple.foundationdb.record.metadata.Key.Expressions.field("watch_id")
    );

    metadataBuilder.addIndex("Watch", new Index(
      "index-key",
      com.apple.foundationdb.record.metadata.Key.Expressions.field("key"),
      IndexTypes.VALUE
    ));

    metadataBuilder.addIndex("Watch", new Index(
      "index-range-end",
      com.apple.foundationdb.record.metadata.Key.Expressions.field("range_end"),
      IndexTypes.VALUE
    ));
  }

  private void setupLease(RecordMetaDataBuilder metadataBuilder) {
    metadataBuilder.getRecordType("Lease").setPrimaryKey(com.apple.foundationdb.record.metadata.Key.Expressions.field("ID"));
  }

  private void setupKeyValue(RecordMetaDataBuilder metadataBuilder) {
    // This can be stored within FDB,
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStoreTest.java#L101
    //
    // Create a primary key composed of the key and version
    metadataBuilder.getRecordType("KeyValue").setPrimaryKey(com.apple.foundationdb.record.metadata.Key.Expressions.concat(
      com.apple.foundationdb.record.metadata.Key.Expressions.field("key"),
      com.apple.foundationdb.record.metadata.Key.Expressions.field("version"))); // Version in primary key not supported, so we need to use our version

    metadataBuilder.addIndex("KeyValue", new Index(
      "keyvalue-modversion", Key.Expressions.field("mod_revision"), IndexTypes.VALUE));

    metadataBuilder.addIndex("KeyValue", new Index(
      "keyvalue-deleted", Key.Expressions.field("is_deleted"), IndexTypes.VALUE));

    metadataBuilder.addIndex("KeyValue", new Index(
      "keyvalue-compaction-index", Key.Expressions.field("remove_during_compaction"), IndexTypes.VALUE));

    // add an index to easily retrieve the max version for a given key, instead of scanning
    metadataBuilder.addIndex("KeyValue", INDEX_VERSION_PER_KEY);

    // keep tracks of keyvalue versionstamp
    metadataBuilder.addIndex("KeyValue", VERSIONSTAMP_INDEX);

    // add a global index that will count all records and updates
    metadataBuilder.addUniversalIndex(COUNT_INDEX);
  }


}
