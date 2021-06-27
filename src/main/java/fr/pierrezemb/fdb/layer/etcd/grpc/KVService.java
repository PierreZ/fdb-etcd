package fr.pierrezemb.fdb.layer.etcd.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.VertxKVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import fr.pierrezemb.fdb.layer.etcd.utils.ProtoUtils;
import io.vertx.core.Future;
import mvccpb.EtcdIoKvProto;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * KVService corresponds to the KV GRPC service
 */
public class KVService extends VertxKVGrpc.KVVertxImplBase {

  private final EtcdRecordLayer recordLayer;

  public KVService(EtcdRecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  @Override
  public Future<EtcdIoRpcProto.RangeResponse> range(EtcdIoRpcProto.RangeRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Auth enabled and tenant not found.");
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

    return Future.succeededFuture(EtcdIoRpcProto.RangeResponse.newBuilder().addAllKvs(kvs).setCount(kvs.size()).build());
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

  @Override
  public Future<EtcdIoRpcProto.PutResponse> put(EtcdIoRpcProto.PutRequest request) {
    EtcdRecord.KeyValue put;

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    if (request.getLease() > 0) {
      EtcdRecord.Lease lease = this.recordLayer.get(tenantId, request.getLease());
      if (null == lease) {
        return Future.failedFuture("etcdserver: requested lease not found");
      }
    }

    try {
      put = this.recordLayer.put(tenantId, ProtoUtils.from(request));
    } catch (InvalidProtocolBufferException e) {
      return Future.failedFuture(e);
    }
    return Future.succeededFuture(EtcdIoRpcProto
      .PutResponse.newBuilder()
      .setHeader(
        EtcdIoRpcProto.ResponseHeader.newBuilder().setRevision(put.getModRevision()).build()
      ).build());
  }

  @Override
  public Future<EtcdIoRpcProto.DeleteRangeResponse> deleteRange(EtcdIoRpcProto.DeleteRangeRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Tenant id not found.");
    }
    Integer count = this.recordLayer.delete(
      tenantId,
      request.getKey().toByteArray(),
      request.getRangeEnd().isEmpty() ? request.getKey().toByteArray() : request.getRangeEnd().toByteArray());

    return Future.succeededFuture(EtcdIoRpcProto.DeleteRangeResponse.newBuilder().setDeleted(count.longValue()).build());
  }

  @Override
  public Future<EtcdIoRpcProto.TxnResponse> txn(EtcdIoRpcProto.TxnRequest request) {
    return super.txn(request);
  }

  @Override
  public Future<EtcdIoRpcProto.CompactionResponse> compact(EtcdIoRpcProto.CompactionRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      return Future.failedFuture("Tenant id not found.");
    }
    this.recordLayer.compact(tenantId, request.getRevision());
    return Future.succeededFuture(EtcdIoRpcProto.CompactionResponse.newBuilder().build());
  }
}
