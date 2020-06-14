package fr.pierrezemb.fdb.layer.etcd.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.KVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.notifier.Notifier;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils;
import io.vertx.core.Promise;
import mvccpb.EtcdIoKvProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * KVService corresponds to the KV GRPC service
 */
public class KVService extends KVGrpc.KVVertxImplBase {

  private final Notifier notifier;
  private final EtcdRecordLayer recordLayer;

  public KVService(EtcdRecordLayer recordLayer, Notifier notifier) {
    this.recordLayer = recordLayer;
    this.notifier = notifier;
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
    EtcdRecord.KeyValue put;

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    if (request.getLease() > 0) {
      EtcdRecord.Lease lease = this.recordLayer.get(tenantId, request.getLease());
      if (null == lease) {
        response.fail(new RuntimeException("etcdserver: requested lease not found"));
        return;
      }
    }

    try {
      put = this.recordLayer.put(tenantId, ProtoUtils.from(request), notifier);
    } catch (InvalidProtocolBufferException e) {
      response.fail(e);
      return;
    }
    response.complete(
      EtcdIoRpcProto
        .PutResponse.newBuilder()
        .setHeader(
          EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(put.getModRevision()).build()
        ).build()
    );
  }

  /**
   * <pre>
   *   range retrieves keys.
   * 	 By default, Get will return the value for "key", if any.
   * 	 When passed WithRange(end), Get will return the keys in the range [key, end).
   * 	 When passed WithFromKey(), Get returns keys greater than or equal to key.
   * 	 When passed WithRev(rev) with rev > 0, Get retrieves keys at the given revision;
   * 	 if the required revision is compacted, the request will fail with ErrCompacted .
   * 	 When passed WithLimit(limit), the number of returned keys is bounded by limit.
   * 	 When passed WithSort(), the keys will be sorted.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void range(EtcdIoRpcProto.RangeRequest request, Promise<EtcdIoRpcProto.RangeResponse> response) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    List<EtcdRecord.KeyValue> results = new ArrayList<>();
    int version = Math.toIntExact(request.getRevision());

    if (request.getRangeEnd().isEmpty()) {
      // get
      results.add(recordLayer.get(tenantId, request.getKey().toByteArray(), version));
    } else {
      // scan
      results = recordLayer.scan(tenantId, request.getKey().toByteArray(), request.getRangeEnd().toByteArray(), version);
    }

    List<EtcdIoKvProto.KeyValue> kvs = results.stream()
      .flatMap(Stream::ofNullable)
      .map(e -> EtcdIoKvProto.KeyValue.newBuilder()
        .setKey(e.getKey()).setValue(e.getValue()).build()).collect(Collectors.toList());

    if (request.getSortOrder().getNumber() > 0) {
      kvs.sort(createComparatorFromRequest(request));
    }

    response.complete(EtcdIoRpcProto.RangeResponse.newBuilder().addAllKvs(kvs).setCount(kvs.size()).build());
  }

  private Comparator<? super EtcdIoKvProto.KeyValue> createComparatorFromRequest(EtcdIoRpcProto.RangeRequest request) {
    Comparator<EtcdIoKvProto.KeyValue> comparator;
    switch (request.getSortTarget()) {
      case KEY:
        comparator = Comparator.comparing(e -> e.getKey().toStringUtf8());
        break;
      case VERSION:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getVersion);
        break;
      case CREATE:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getCreateRevision);
        break;
      case MOD:
        comparator = Comparator.comparing(EtcdIoKvProto.KeyValue::getModRevision);
        break;
      case VALUE:
        comparator = Comparator.comparing(e -> e.getValue().toStringUtf8());
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + request.getSortTarget());
    }

    if (request.getSortOrder().equals(EtcdIoRpcProto.RangeRequest.SortOrder.DESCEND)) {
      comparator = comparator.reversed();
    }

    return comparator;
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
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Tenant id not found.");
    }
    Integer count = this.recordLayer.delete(
      tenantId,
      request.getKey().toByteArray(),
      request.getRangeEnd().isEmpty() ? request.getKey().toByteArray() : request.getRangeEnd().toByteArray());
    response.complete(EtcdIoRpcProto.DeleteRangeResponse.newBuilder().setDeleted(count.longValue()).build());
  }

  /**
   * <pre>
   * Compact compacts the event history in the etcd key-value store. The key-value
   * store should be periodically compacted or the event history will continue to grow
   * indefinitely.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void compact(EtcdIoRpcProto.CompactionRequest request, Promise<EtcdIoRpcProto.CompactionResponse> response) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    this.recordLayer.compact(tenantId, request.getRevision());
    response.complete();
  }
}
