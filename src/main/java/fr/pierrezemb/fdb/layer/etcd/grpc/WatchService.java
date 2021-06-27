package fr.pierrezemb.fdb.layer.etcd.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.WatchGrpc;
import fr.pierrezemb.fdb.layer.etcd.WatchVerticle;
import fr.pierrezemb.fdb.layer.etcd.store.EtcdRecordLayer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import mvccpb.EtcdIoKvProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class WatchService extends WatchGrpc.WatchImplBase {
  private static final Logger log = LoggerFactory.getLogger(WatchService.class);
  private final EtcdRecordLayer recordLayer;
  private final Random random;
  private final Vertx vertx;
  private final Map<String, Verticle> verticleMap;

  public WatchService(EtcdRecordLayer etcdRecordLayer, Vertx vertx) {
    this.vertx = vertx;
    this.recordLayer = etcdRecordLayer;
    random = new Random(System.currentTimeMillis());
    verticleMap = new HashMap<>();
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
        log.debug("received a watchRequest {}", request);
        switch (request.getRequestUnionCase()) {

          case CREATE_REQUEST:

            EtcdIoRpcProto.WatchCreateRequest createRequest = request.getCreateRequest();
            if (createRequest.getWatchId() == 0) {
              createRequest = createRequest.toBuilder().setWatchId(random.nextLong()).build();
            }

            handleCreateRequest(createRequest, tenantId, responseObserver);
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
    log.info("cancel watch");
    this.recordLayer.deleteWatch(tenantId, cancelRequest.getWatchId());
  }

  private void handleCreateRequest(EtcdIoRpcProto.WatchCreateRequest createRequest, String tenantId, StreamObserver<EtcdIoRpcProto.WatchResponse> responseObserver) {

    long commitVersion = this.recordLayer.put(tenantId, createRequest);
    log.info("successfully registered new Watch");

    String address = tenantId + createRequest.getWatchId();

    WatchVerticle verticle = new WatchVerticle(tenantId, createRequest.getWatchId(), recordLayer, commitVersion, createRequest.getKey(), createRequest.getRangeEnd());
    vertx.deployVerticle(verticle, res -> {
      if (res.succeeded()) {
        log.debug("Deployment id is {}", res.result());
        this.verticleMap.put(address, verticle);

        // and then watch for events
        this.vertx.eventBus().consumer(address, message -> {
          if (!(message.body() instanceof byte[])) {
            return;
          }

          try {
            EtcdIoKvProto.Event event = EtcdIoKvProto.Event.newBuilder().mergeFrom((byte[]) message.body()).build();
            responseObserver
              .onNext(EtcdIoRpcProto.WatchResponse.newBuilder()
                .setHeader(EtcdIoRpcProto.ResponseHeader.newBuilder().build())
                .addEvents(event)
                .setWatchId(createRequest.getWatchId())
                .build());
          } catch (StatusRuntimeException e) {
            if (e.getStatus().equals(Status.CANCELLED)) {
              log.warn("connection was closed, closing verticle");
              return;
            }
            log.error("cought an error writing response: {}", e.getMessage());
          } catch (InvalidProtocolBufferException e) {
            log.warn("cannot deserialize, skipping");
          }
        });
      } else {
        log.error("Deployment failed: ", res.cause());
      }
    });
  }
}
