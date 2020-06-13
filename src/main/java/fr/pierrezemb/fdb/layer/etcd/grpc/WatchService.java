package fr.pierrezemb.fdb.layer.etcd.grpc;

import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.WatchGrpc;
import fr.pierrezemb.fdb.layer.etcd.notifier.Notifier;
import fr.pierrezemb.fdb.layer.etcd.service.RecordServiceBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchService extends WatchGrpc.WatchImplBase {
  private static final Logger log = LoggerFactory.getLogger(WatchService.class);
  private final RecordServiceBuilder recordServiceBuilder;
  private final Notifier notifier;

  public WatchService(RecordServiceBuilder recordServiceBuilder, Notifier notifier) {
    this.notifier = notifier;
    this.recordServiceBuilder = recordServiceBuilder;
  }

  @Override
  public StreamObserver<EtcdIoRpcProto.WatchRequest> watch(StreamObserver<EtcdIoRpcProto.WatchResponse> responseObserver) {

    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    return new StreamObserver<EtcdIoRpcProto.WatchRequest>() {

      @Override
      public void onNext(EtcdIoRpcProto.WatchRequest request) {
        switch (request.getRequestUnionCase()) {

          case CREATE_REQUEST:
            handleCreateRequest(request.getCreateRequest(), tenantId);
            break;
          case CANCEL_REQUEST:
            handleCancelRequest(request.getCancelRequest(), tenantId);
            break;
          case PROGRESS_REQUEST:
            log.warn("received a progress request");
            break;
          case REQUESTUNION_NOT_SET:
            log.warn("received an empty watch request");
            break;
        }
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    };
  }

  private void handleCancelRequest(EtcdIoRpcProto.WatchCancelRequest cancelRequest, String tenantId) {
    this.recordServiceBuilder.withTenant(tenantId).watch.delete(cancelRequest.getWatchId());
  }

  private void handleCreateRequest(EtcdIoRpcProto.WatchCreateRequest createRequest, String tenantId) {
    this.recordServiceBuilder.withTenant(tenantId).watch.put(createRequest);
    log.info("successfully registered new Watch");
  }

}
