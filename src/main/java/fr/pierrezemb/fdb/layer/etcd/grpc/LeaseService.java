package fr.pierrezemb.fdb.layer.etcd.grpc;

import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.LeaseGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.service.RecordServiceBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LeaseService corresponds to the Lease GRCP service
 */
public class LeaseService extends LeaseGrpc.LeaseImplBase {
  private static final Logger log = LoggerFactory.getLogger(LeaseService.class);
  private final RecordServiceBuilder recordServiceBuilder;

  public LeaseService(RecordServiceBuilder recordServiceBuilder) {
    this.recordServiceBuilder = recordServiceBuilder;
  }

  /**
   * <pre>
   * LeaseGrant creates a lease which expires if the server does not receive a keepAlive
   * within a given time to live period. All keys attached to the lease will be expired and
   * deleted if the lease expires. Each expired key generates a delete event in the event history.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void leaseGrant(EtcdIoRpcProto.LeaseGrantRequest request, StreamObserver<EtcdIoRpcProto.LeaseGrantResponse> responseObserver) {
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

    recordServiceBuilder.withTenant(tenantId).lease.put(lease);
    responseObserver
      .onNext(EtcdIoRpcProto
        .LeaseGrantResponse.newBuilder()
        .setID(lease.getID())
        .setTTL(lease.getTTL())
        .build());
    responseObserver.onCompleted();
  }

  /**
   * <pre>
   * LeaseRevoke revokes a lease. All keys attached to the lease will expire and be deleted.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void leaseRevoke(EtcdIoRpcProto.LeaseRevokeRequest request, StreamObserver<EtcdIoRpcProto.LeaseRevokeResponse> responseObserver) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    recordServiceBuilder.withTenant(tenantId).lease.delete(request.getID());
    recordServiceBuilder.withTenant(tenantId).kv.delete(request.getID());
    responseObserver.onNext(EtcdIoRpcProto.LeaseRevokeResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * <pre>
   * LeaseKeepAlive keeps the lease alive by streaming keep alive requests from the client
   * to the server and streaming keep alive responses from the server to the client.
   * </pre>
   *
   * @param responseObserver
   */
  @Override
  public StreamObserver<EtcdIoRpcProto.LeaseKeepAliveRequest> leaseKeepAlive(StreamObserver<EtcdIoRpcProto.LeaseKeepAliveResponse> responseObserver) {
    return new StreamObserver<EtcdIoRpcProto.LeaseKeepAliveRequest>() {
      @Override
      public void onNext(EtcdIoRpcProto.LeaseKeepAliveRequest value) {
        String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
        if (tenantId == null) {
          throw new RuntimeException("Auth enabled and tenant not found.");
        }
        if (log.isTraceEnabled()) {
          log.trace("receive a keepAlive for lease {}", value.getID());
        }
        EtcdRecord.Lease record = recordServiceBuilder.withTenant(tenantId).lease.keepAlive(value.getID());
        if (log.isTraceEnabled()) {
          log.trace("lease {} updated", value.getID());
          responseObserver.onNext(EtcdIoRpcProto.LeaseKeepAliveResponse.newBuilder()
            .setID(record.getID())
            .setTTL(record.getTTL()).build());
        }
      }

      @Override
      public void onError(Throwable t) {
        log.error(t.getLocalizedMessage());
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  /**
   * <pre>
   * LeaseTimeToLive retrieves lease information.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void leaseTimeToLive(EtcdIoRpcProto.LeaseTimeToLiveRequest request, StreamObserver<EtcdIoRpcProto.LeaseTimeToLiveResponse> responseObserver) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    EtcdRecord.Lease lease = recordServiceBuilder.withTenant(tenantId).lease.get(request.getID());
    List<EtcdRecord.KeyValue> records = recordServiceBuilder.withTenant(tenantId).kv.getWithLease(request.getID());
    responseObserver.onNext(EtcdIoRpcProto.LeaseTimeToLiveResponse.newBuilder()
      .setID(request.getID())
      .setTTL(lease.getTTL())
      .addAllKeys(records.stream().map(EtcdRecord.KeyValue::getKey).collect(Collectors.toList()))
      .setGrantedTTL(lease.getTTL())
      .build());
    responseObserver.onCompleted();
  }

  /**
   * <pre>
   * LeaseLeases lists all existing leases.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void leaseLeases(EtcdIoRpcProto.LeaseLeasesRequest request, StreamObserver<EtcdIoRpcProto.LeaseLeasesResponse> responseObserver) {
    super.leaseLeases(request, responseObserver);
  }
}
