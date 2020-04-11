package fr.pierrezemb.fdb.layer.etcd.service;

import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.LeaseGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.RandomUtils;

/**
 * LeaseService corresponds to the Lease GRCP service
 */
public class LeaseService extends LeaseGrpc.LeaseImplBase {
  private final RecordService recordService;

  public LeaseService(RecordService recordService) {
    this.recordService = recordService;
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

    EtcdRecord.Lease lease = EtcdRecord.Lease.newBuilder()
      .setTTL(request.getTTL())
      .setID(id)
      .setInsertTimestamp(System.currentTimeMillis())
      .build();

    recordService.lease.put(lease);
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
    super.leaseRevoke(request, responseObserver);
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
    return super.leaseKeepAlive(responseObserver);
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
    super.leaseTimeToLive(request, responseObserver);
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
