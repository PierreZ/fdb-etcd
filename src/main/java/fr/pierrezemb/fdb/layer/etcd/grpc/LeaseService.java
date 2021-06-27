package fr.pierrezemb.fdb.layer.etcd.grpc;

import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.VertxLeaseGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import io.vertx.core.Future;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * LeaseService corresponds to the Lease GRCP service
 */
public class LeaseService extends VertxLeaseGrpc.LeaseVertxImplBase {
  private static final Logger log = LoggerFactory.getLogger(LeaseService.class);
  private final EtcdRecordLayer recordLayer;

  public LeaseService(EtcdRecordLayer etcdRecordLayer) {
    this.recordLayer = etcdRecordLayer;
  }

  @Override
  public Future<EtcdIoRpcProto.LeaseGrantResponse> leaseGrant(EtcdIoRpcProto.LeaseGrantRequest request) {
    long id = request.getID();
    if (id == 0) {
      id = RandomUtils.nextLong();
    }

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    EtcdRecord.Lease lease = EtcdRecord.Lease.newBuilder()
      .setTTL(request.getTTL())
      .setID(id)
      .setInsertTimestamp(System.currentTimeMillis())
      .build();

    this.recordLayer.put(tenantId, lease);

    return Future.succeededFuture(EtcdIoRpcProto
      .LeaseGrantResponse.newBuilder()
      .setID(lease.getID())
      .setTTL(lease.getTTL())
      .build());
  }

  @Override
  public Future<EtcdIoRpcProto.LeaseRevokeResponse> leaseRevoke(EtcdIoRpcProto.LeaseRevokeRequest request) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    this.recordLayer.deleteLease(tenantId, request.getID());
    this.recordLayer.deleteRecordsWithLease(tenantId, request.getID());
    return Future.succeededFuture(EtcdIoRpcProto.LeaseRevokeResponse.newBuilder().build());
  }

  @Override
  public Future<EtcdIoRpcProto.LeaseTimeToLiveResponse> leaseTimeToLive(EtcdIoRpcProto.LeaseTimeToLiveRequest request) {

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }

    EtcdRecord.Lease lease = recordLayer.get(tenantId, request.getID());
    List<EtcdRecord.KeyValue> records = recordLayer.getWithLease(tenantId, request.getID());
    return Future.succeededFuture(EtcdIoRpcProto.LeaseTimeToLiveResponse.newBuilder()
      .setID(request.getID())
      .setTTL(lease.getTTL())
      .addAllKeys(records.stream().map(EtcdRecord.KeyValue::getKey).collect(Collectors.toList()))
      .setGrantedTTL(lease.getTTL())
      .build());
  }

  @Override
  public Future<EtcdIoRpcProto.LeaseLeasesResponse> leaseLeases(EtcdIoRpcProto.LeaseLeasesRequest request) {
    // TODO
    return super.leaseLeases(request);
  }

  @Override
  public void leaseKeepAlive(ReadStream<EtcdIoRpcProto.LeaseKeepAliveRequest> request, WriteStream<EtcdIoRpcProto.LeaseKeepAliveResponse> response) {
    request.handler(value -> {
      String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
      if (tenantId == null) {
        throw new RuntimeException("Auth enabled and tenant not found.");
      }
      if (log.isTraceEnabled()) {
        log.trace("receive a keepAlive for lease {}", value.getID());
      }
      EtcdRecord.Lease record = recordLayer.keepAlive(tenantId, value.getID());
      if (log.isTraceEnabled()) {
        log.trace("lease {} updated", value.getID());
        response.write(EtcdIoRpcProto.LeaseKeepAliveResponse.newBuilder()
          .setID(record.getID())
          .setTTL(record.getTTL()).build());
      }
    });
  }
}
