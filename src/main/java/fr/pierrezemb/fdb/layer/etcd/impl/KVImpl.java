package fr.pierrezemb.fdb.layer.etcd.impl;


import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Utf8;
import com.google.protobuf.Message;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.KVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.EtcdRecordStore;
import io.vertx.core.Promise;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;

public class KVImpl extends KVGrpc.KVVertxImplBase {

  private EtcdRecordStore recordStore;

  public KVImpl(EtcdRecordStore recordStore) {
    this.recordStore = recordStore;
  }

  /**
   * <pre>
   * Put puts the given key into the key-value store.
   * A put request increments the revision of the key-value store
   * and generates one event in the event history.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void put(EtcdIoRpcProto.PutRequest request, Promise<EtcdIoRpcProto.PutResponse> response) {
    this.recordStore.db.run(context -> {
      FDBRecordStore recordStore = this.recordStore.recordStoreProvider.apply(context);
      recordStore.saveRecord(EtcdRecord.PutRequest.newBuilder()
        .setKey(request.getKey())
        .setValue(request.getValue())
        .setLease(request.getLease())
        .setPrevKv(request.getPrevKv())
        .setIgnoreValue(request.getIgnoreValue())
        .setIgnoreLease(request.getIgnoreLease()).build());
      return null;
    });
    response.complete();
  }

  /**
   * <pre>
   * Range gets the keys in the range from the key-value store.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void range(EtcdIoRpcProto.RangeRequest request, Promise<EtcdIoRpcProto.RangeResponse> response) {

    // Subspace(rawPrefix=\x02record-layer-demo\x00\x15\x01)
    List<EtcdRecord.PutRequest> putRequestList = this.recordStore.db.run(context -> {
      RecordCursor<FDBStoredRecord<Message>> cursor =
        this.recordStore.recordStoreProvider
          .apply(context)
          .scanRecords(
            Tuple.from(request.getKey().toStringUtf8()),
            null,
            // Tuple.from(request.getRangeEnd().toStringUtf8()),
            EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE,
            null, ScanProperties.FORWARD_SCAN);

      return cursor
        .map(queriedRecord -> EtcdRecord.PutRequest.newBuilder().mergeFrom(queriedRecord.getRecord()).build())
        .asList().join();
    });
    response.complete();
  }
}
