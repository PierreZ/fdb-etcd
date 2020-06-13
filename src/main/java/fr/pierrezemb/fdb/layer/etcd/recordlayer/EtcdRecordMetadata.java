package fr.pierrezemb.fdb.layer.etcd.recordlayer;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * EtcdRecordStore jobs is to generate/handle and expose the recordStoreProvider and fdb
 */
public class EtcdRecordMetadata {

  // Keep a global track of the number of records stored
  protected static final Index COUNT_INDEX = new Index(
    "globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);

  // keep track of the version per key with an index
  protected static final Index INDEX_VERSION_PER_KEY = new Index(
    "index-version-per-key",
    Key.Expressions.field("version").groupBy(Key.Expressions.field("key")),
    IndexTypes.MAX_EVER_LONG);

  private static final Logger log = LoggerFactory.getLogger(EtcdRecordMetadata.class);
  public final FDBDatabase db;
  public final Function<FDBRecordContext, FDBRecordStore> recordStoreProvider;
  // In this case, the key space implies that there are multiple "applications"
  // that might be defined to run on the same FoundationDB cluster, and then
  // each "application" might have multiple "environments". This could be used,
  // for example, to connect to either the "prod" or "qa" environment for the same
  // application from within the same code base.
  private final KeySpace keySpace = new KeySpace(
    new DirectoryLayerDirectory("application")
      .addSubdirectory(new KeySpaceDirectory("tenant", KeySpaceDirectory.KeyType.STRING))
  );
  private final KeySpacePath path;

  public EtcdRecordMetadata(FDBDatabase db, String tenant) {
    this.db = db;


    // Create a path for the "etcd-store" application's and "user1" user
    path = keySpace.path("application", "etcd-store").add("tenant", tenant);

    RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
      .setRecords(EtcdRecord.getDescriptor());

    setupKeyValue(metadataBuilder);
    setupLease(metadataBuilder);
    setupWatch(metadataBuilder);

    // record-layer has the capacity to set versions on each records.
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/docs/Overview.md#indexing-by-version
    metadataBuilder.setStoreRecordVersions(true);

    recordStoreProvider = context -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metadataBuilder)
      .setContext(context)
      .setKeySpacePath(path)
      .createOrOpen();
  }

  private void setupWatch(RecordMetaDataBuilder metadataBuilder) {
    metadataBuilder.getRecordType("Watch").setPrimaryKey(
      Key.Expressions.field("watch_id")
    );

    metadataBuilder.addIndex("Watch", new Index(
      "index-key",
      Key.Expressions.field("key"),
      IndexTypes.VALUE
    ));

    metadataBuilder.addIndex("Watch", new Index(
      "index-range-end",
      Key.Expressions.field("range_end"),
      IndexTypes.VALUE
    ));
  }

  private void setupLease(RecordMetaDataBuilder metadataBuilder) {
    metadataBuilder.getRecordType("Lease").setPrimaryKey(Key.Expressions.field("ID"));
  }

  private void setupKeyValue(RecordMetaDataBuilder metadataBuilder) {
    // This can be stored within FDB,
    // see https://github.com/FoundationDB/fdb-record-layer/blob/master/fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/FDBMetaDataStoreTest.java#L101
    //
    // Create a primary key composed of the key and version
    metadataBuilder.getRecordType("KeyValue").setPrimaryKey(Key.Expressions.concat(
      Key.Expressions.field("key"),
      Key.Expressions.field("version"))); // Version in primary key not supported, so we need to use our version

    metadataBuilder.addIndex("KeyValue", new Index(
      "keyvalue-modversion", Key.Expressions.field("mod_revision"), IndexTypes.VALUE));

    // add an index to easily retrieve the max version for a given key, instead of scanning
    metadataBuilder.addIndex("KeyValue", INDEX_VERSION_PER_KEY);

    // add a global index that will count all records and updates
    metadataBuilder.addUniversalIndex(COUNT_INDEX);
  }
}
