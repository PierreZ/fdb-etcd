package fr.pierrezemb.fdb.layer.etcd.service;

import etcdserverpb.AuthGrpc;
import etcdserverpb.EtcdIoRpcProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthService extends AuthGrpc.AuthImplBase {
  private static Logger log = LoggerFactory.getLogger(AuthService.class);
  private final RecordService recordService;

  public AuthService(RecordService recordService) {
    this.recordService = recordService;
  }

  /**
   * <pre>
   * AuthStatus displays authentication status.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void authStatus(EtcdIoRpcProto.AuthStatusRequest request, StreamObserver<EtcdIoRpcProto.AuthStatusResponse> responseObserver) {
    responseObserver.onNext(EtcdIoRpcProto.AuthStatusResponse.newBuilder().setEnabled(true).build());
  }

  /**
   * <pre>
   * Authenticate processes an authenticate request.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void authenticate(EtcdIoRpcProto.AuthenticateRequest request, StreamObserver<EtcdIoRpcProto.AuthenticateResponse> responseObserver) {
    log.trace("using user '{}'", request.getName());
    responseObserver.onNext(EtcdIoRpcProto.AuthenticateResponse.newBuilder().setToken(request.getName()).build());
    responseObserver.onCompleted();
  }
}
