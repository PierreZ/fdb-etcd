package fr.pierrezemb.fdb.layer.etcd.recordlayer;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.notifier.Notifier;
import mvccpb.EtcdIoKvProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.apple.foundationdb.record.TupleRange.ALL;
import static fr.pierrezemb.fdb.layer.etcd.recordlayer.EtcdRecordMetadata.COUNT_INDEX;
import static fr.pierrezemb.fdb.layer.etcd.recordlayer.EtcdRecordMetadata.INDEX_VERSION_PER_KEY;

/**
 * KVRecordStore is handling all things related to the KVService on the record-layer
 */
public class KVRecordStore {
  private static final Logger log = LoggerFactory.getLogger(KVRecordStore.class);

  private final EtcdRecordMetadata etcdRecordMetadata;

  public KVRecordStore(EtcdRecordMetadata etcdRecordMetadata) {
    this.etcdRecordMetadata = etcdRecordMetadata;
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
    List<EtcdRecord.KeyValue> records = etcdRecordMetadata.db.run(context -> {
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

    long now = System.currentTimeMillis();
    // this returns an asynchronous cursor over the results of our query
    return etcdRecordMetadata.recordStoreProvider
      .apply(context)
      // this returns an asynchronous cursor over the results of our query
      .executeQuery(query)
      .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
        .mergeFrom(queriedRecord.getRecord()).build())
      // filter according to the lease
      .filter(record -> {
        if (record.getLease() == 0) {
          // no lease
          return true;
        }
        // the record has a lease, retrieve it
        FDBStoredRecord<Message> leaseMsg = this.etcdRecordMetadata.recordStoreProvider
          .apply(context)
          .loadRecord(Tuple.from(record.getLease()));
        if (leaseMsg == null) {
          log.debug("record '{}' has a lease '{}' that can not be found, filtering",
            record.getKey().toStringUtf8(), record.getLease());
          return false;
        }
        EtcdRecord.Lease lease = EtcdRecord.Lease.newBuilder().mergeFrom(leaseMsg.getRecord()).build();
        if (now > lease.getInsertTimestamp() + lease.getTTL() * 1000) {
          log.debug("record '{}' has a lease '{}' that has expired, filtering",
            record.getKey().toStringUtf8(), record.getLease());
          return false;
        }

        return true;
      }).asList().join();

  }

  public List<EtcdRecord.KeyValue> scan(byte[] start, byte[] end) {
    return scan(start, end, 0);
  }

  public List<EtcdRecord.KeyValue> scan(byte[] start, byte[] end, long revision) {
    log.trace("scanning between {} and {} with revision {}", start, end, revision);
    return etcdRecordMetadata.db.run(context -> {
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

  public List<EtcdRecord.KeyValue> getWithLease(long id) {
    log.trace("retrieving record for revision {}", id);
    return etcdRecordMetadata.db.run(context -> {
      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(
          Query.field("lease").equalsValue(id)
        ).build();
      return runQuery(context, query);
    });
  }

  public EtcdRecord.KeyValue put(EtcdRecord.KeyValue record) {
    return put(record, null);
  }

  public EtcdRecord.KeyValue put(EtcdRecord.KeyValue record, Notifier notifier) {
    return this.etcdRecordMetadata.db.run(context -> {

      // retrieve version using an index
      IndexAggregateFunction function = new IndexAggregateFunction(
        FunctionNames.MAX_EVER, INDEX_VERSION_PER_KEY.getRootExpression(), INDEX_VERSION_PER_KEY.getName());
      Tuple maxResult = etcdRecordMetadata.recordStoreProvider.apply(context)
        .evaluateAggregateFunction(
          Collections.singletonList("KeyValue"), function,
          Key.Evaluated.concatenate(record.getKey().toByteArray()), IsolationLevel.SERIALIZABLE)
        .join();

      if (log.isTraceEnabled()) {
        // debug: let's scan and print the index to see how it is built:
        etcdRecordMetadata.recordStoreProvider.apply(context).scanIndex(
          INDEX_VERSION_PER_KEY,
          IndexScanType.BY_GROUP,
          ALL,
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
      FDBRecordStore recordStore = etcdRecordMetadata.recordStoreProvider.apply(context);
      recordStore.saveRecord(fixedRecord);
      log.trace("successfully put record {} in version {}", fixedRecord.toString(), fixedRecord.getVersion());

//      // checking if we have a Watch underneath
//      RecordQuery watchQuery = RecordQuery
//        .newBuilder()
//        .setRecordType("Watch")
//        .setFilter(
//          Query.or(
//            // watch a single key
//            Query.and(
//              Query.field("range_end").isNull(),
//              Query.field("key").equalsValue(fixedRecord.getKey().toByteArray())
//            ),
//              // watching over a range
//              Query.and(
//                Query.field("key").lessThanOrEquals(fixedRecord.getKey().toByteArray()),
//                Query.field("range_end").greaterThanOrEquals(fixedRecord.getKey().toByteArray())
//              )
//            )
//        ).build();
      RecordQuery watchQuery = RecordQuery
        .newBuilder()
        .setRecordType("Watch")
        .setFilter(
            // watch a single key
            Query.and(
              Query.field("range_end").isNull(),
              Query.field("key").equalsValue(fixedRecord.getKey().toByteArray())
            )
        ).build();

      List<EtcdRecord.Watch> toto = recordStore.executeQuery(watchQuery)
        .map(queriedRecord -> EtcdRecord.Watch.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .asList().join();

      log.info("@@@@@@@@@@@@@@@@@@@@@ Found {} watchs", toto.size());

      if (notifier != null && toto.size() > 0) {
        toto.forEach(w -> {
          log.debug("found a matching watch {}", w);
          EtcdIoKvProto.Event event = EtcdIoKvProto.Event.newBuilder()
            .setType(EtcdIoKvProto.Event.EventType.PUT)
            .setKv(EtcdIoKvProto.KeyValue.newBuilder()
              .setKey(fixedRecord.getKey())
              .setValue(fixedRecord.getValue())
              .setLease(fixedRecord.getLease())
              .setModRevision(fixedRecord.getModRevision())
              .setVersion(fixedRecord.getModRevision())
              .build())
            .build();

          notifier.publish("default", w.getWatchId(), event);
        });
      } else {
        log.warn("found no matching watch");
      }

      return fixedRecord;
    });
  }

  public Integer delete(byte[] start, byte[] end) {
    Integer count = this.etcdRecordMetadata.db.run(context -> {
      log.trace("deleting between {} and {}", start, end);
      FDBRecordStore recordStore = etcdRecordMetadata.recordStoreProvider.apply(context);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.and(
          Query.field("key").greaterThanOrEquals(start),
          Query.field("key").lessThanOrEquals(end)
        )).build();

      return etcdRecordMetadata.recordStoreProvider
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
    Integer count = this.etcdRecordMetadata.db.run(context -> {
      log.warn("compacting any record before {}", revision);
      FDBRecordStore recordStore = etcdRecordMetadata.recordStoreProvider.apply(context);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("mod_revision").lessThanOrEquals(revision)).build();

      return etcdRecordMetadata.recordStoreProvider
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
    Long stat = this.etcdRecordMetadata.db.run(context -> {
      IndexAggregateFunction function = new IndexAggregateFunction(
        FunctionNames.COUNT, COUNT_INDEX.getRootExpression(), COUNT_INDEX.getName());
      FDBRecordStore recordStore = etcdRecordMetadata.recordStoreProvider.apply(context);

      if (log.isTraceEnabled()) {
        // debug: let's scan and print the index to see how it is built:
        this.etcdRecordMetadata.recordStoreProvider.apply(context).scanIndex(
          COUNT_INDEX,
          IndexScanType.BY_GROUP,
          ALL,
          null, // continuation,
          ScanProperties.FORWARD_SCAN
        ).asList().join().stream().forEach(
          indexEntry -> log.trace("found an indexEntry for stats: key:'{}', value: '{}'", indexEntry.getKey(), indexEntry.getValue())
        );
      }

      return recordStore.evaluateAggregateFunction(EvaluationContext.EMPTY, Collections.singletonList("KeyValue"), function, ALL, IsolationLevel.SERIALIZABLE)
        .thenApply(tuple -> tuple.getLong(0));
    }).join();

    log.info("we have around {} ETCD records", stat);

    return stat;
  }

  public void delete(long leaseID) {
    log.info("deleting all records with lease '{}'", leaseID);
    Integer count = this.etcdRecordMetadata.db.run(context -> {
      FDBRecordStore recordStore = etcdRecordMetadata.recordStoreProvider.apply(context);

      RecordQuery query = RecordQuery.newBuilder()
        .setRecordType("KeyValue")
        .setFilter(Query.field("lease").equalsValue(leaseID)).build();

      return etcdRecordMetadata.recordStoreProvider
        .apply(context)
        // this returns an asynchronous cursor over the results of our query
        .executeQuery(query)
        .map(queriedRecord -> EtcdRecord.KeyValue.newBuilder()
          .mergeFrom(queriedRecord.getRecord()).build())
        .map(r -> {
          log.trace("found a record to delete for lease {}: {}", r, leaseID);
          return r;
        })
        .map(record -> recordStore.deleteRecord(Tuple.from(record.getKey().toByteArray(), record.getVersion())))
        .getCount()
        .join();
    });
    log.trace("deleted {} records with lease {}", count, leaseID);
  }

}
