package fr.pierrezemb.fdb.layer.etcd.grpc;

import etcdserverpb.AuthGrpc;
import etcdserverpb.EtcdIoRpcProto;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.service.RecordServiceBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthService extends AuthGrpc.AuthImplBase {
  private static Logger log = LoggerFactory.getLogger(AuthService.class);
  private final RecordServiceBuilder recordServiceBuilder;

  public AuthService(RecordServiceBuilder recordServiceBuilder) {
    this.recordServiceBuilder = recordServiceBuilder;
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

  /**
   * <pre>
   * RoleAdd adds a new role. Role name cannot be empty.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleAdd(EtcdIoRpcProto.AuthRoleAddRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleAddResponse> responseObserver) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (!tenantId.equals("root")) {
      throw new RuntimeException("Only root user can create roles and user");
    }
    this.recordServiceBuilder
      .withTenant(tenantId)
      .auth.roleAdd(
      EtcdRecord.Role.newBuilder()
        .setName(request.getNameBytes())
        .build());
    responseObserver.onNext(EtcdIoRpcProto.AuthRoleAddResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  /**
   * <pre>
   * RoleGet gets detailed role information.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleGet(EtcdIoRpcProto.AuthRoleGetRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleGetResponse> responseObserver) {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (!tenantId.equals("root")) {
      throw new RuntimeException("Only root user can create roles and user");
    }
    EtcdRecord.Role role = this.recordServiceBuilder.withTenant(tenantId).auth.getRole(request.getRole());
    responseObserver.onNext(EtcdIoRpcProto.AuthRoleGetResponse.newBuilder().build());
  }

  /**
   * <pre>
   * RoleList gets lists of all roles.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleList(EtcdIoRpcProto.AuthRoleListRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleListResponse> responseObserver) {
    super.roleList(request, responseObserver);
  }

  /**
   * <pre>
   * RoleDelete deletes a specified role.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleDelete(EtcdIoRpcProto.AuthRoleDeleteRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleDeleteResponse> responseObserver) {
    super.roleDelete(request, responseObserver);
  }

  /**
   * <pre>
   * RoleGrantPermission grants a permission of a specified key or range to a specified role.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleGrantPermission(EtcdIoRpcProto.AuthRoleGrantPermissionRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleGrantPermissionResponse> responseObserver) {
    super.roleGrantPermission(request, responseObserver);
  }

  /**
   * <pre>
   * RoleRevokePermission revokes a key or range permission of a specified role.
   * </pre>
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void roleRevokePermission(EtcdIoRpcProto.AuthRoleRevokePermissionRequest request, StreamObserver<EtcdIoRpcProto.AuthRoleRevokePermissionResponse> responseObserver) {
    super.roleRevokePermission(request, responseObserver);
  }
}
