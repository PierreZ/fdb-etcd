package fr.pierrezemb.fdb.layer.etcd.service;


import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.KVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordStore;
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import mvccpb.EtcdIoKvProto;

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

    List<EtcdRecord.PutRequest> results = new ArrayList<>();

    if (request.getRangeEnd().isEmpty()) {
      // get
      results.add(this.recordStore.get(Tuple.from(request.getKey().toByteArray())));
    } else {
      // scan
      results = this.recordStore.scan(
        Tuple.from(request.getKey().toByteArray()), Tuple.from(request.getRangeEnd().toByteArray()));
    }

    // TODO(PZ): convert PutRequest in KeyValue
    List<EtcdIoKvProto.KeyValue> kvs = results.stream()
      .flatMap(Stream::ofNullable)
      .map(e -> EtcdIoKvProto.KeyValue.newBuilder()
        .setKey(e.getKey()).setValue(e.getValue()).build()).collect(Collectors.toList());

    response.complete(EtcdIoRpcProto.RangeResponse.newBuilder().addAllKvs(kvs).build());
  }

  /**
   * <pre>
   * DeleteRange deletes the given range from the key-value store.
   * A delete request increments the revision of the key-value store
   * and generates a delete event in the event history for every deleted key.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void deleteRange(EtcdIoRpcProto.DeleteRangeRequest request, Promise<EtcdIoRpcProto.DeleteRangeResponse> response) {
    this.recordStore.delete(Tuple.from(request.getKey().toByteArray()), Tuple.from(request.getKey().toByteArray()));
    response.complete();
  }
}
