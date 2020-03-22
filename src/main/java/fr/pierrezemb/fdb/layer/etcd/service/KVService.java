package fr.pierrezemb.fdb.layer.etcd.service;


import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.KVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordStore;
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils;
import io.vertx.core.Promise;
import java.util.List;

public class KVService extends KVGrpc.KVVertxImplBase {

  private EtcdRecordStore recordStore;

  public KVService(EtcdRecordStore recordStore) {
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
    try {
      this.recordStore.put(ProtoUtils.from(request));
    } catch (InvalidProtocolBufferException e) {
      response.fail(e);
      return;
    }
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
