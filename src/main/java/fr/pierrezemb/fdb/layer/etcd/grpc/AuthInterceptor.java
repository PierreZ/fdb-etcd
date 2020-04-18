package fr.pierrezemb.fdb.layer.etcd.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthInterceptor implements ServerInterceptor {
  private static final Logger log = LoggerFactory.getLogger(AuthInterceptor.class);
  private final Metadata.Key<String> tokenKey =
    Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER);
  private final boolean authEnabled;
  private final String defaultTenant;

  public AuthInterceptor(boolean authEnabled, String defaultTenant) {
    this.authEnabled = authEnabled;
    this.defaultTenant = defaultTenant;
  }

  /**
   * Intercept {@link ServerCall} dispatch by the {@code next} {@link ServerCallHandler}. General
   * semantics of {@link ServerCallHandler#startCall} apply and the returned
   * {@link ServerCall.Listener} must not be {@code null}.
   *
   * <p>If the implementation throws an exception, {@code call} will be closed with an error.
   * Implementations must not throw an exception if they started processing that may use {@code
   * call} on another thread.
   *
   * @param call    object to receive response messages
   * @param headers which can contain extra call metadata from {@link ClientCall#start},
   *                e.g. authentication credentials.
   * @param next    next processor in the interceptor chain
   * @return listener for processing incoming messages for {@code call}, never {@code null}.
   */
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    if (!authEnabled) {
      if (log.isTraceEnabled()) {
        log.trace("setting tenant to {}", defaultTenant);
      }
      Context context = Context.current().withValue(GrpcContextKeys.TENANT_ID_KEY, defaultTenant);
      return Contexts.interceptCall(context, call, headers, next);
    }

    // we need to allow calls to AuthService to retrieve token
    if (call.getMethodDescriptor().getFullMethodName().equals("etcdserverpb.Auth/Authenticate")) {
      return next.startCall(call, headers);
    }

    if (!headers.containsKey(tokenKey)) {
      call.close(Status.PERMISSION_DENIED.withDescription("no authorization token"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }

    final String authToken = headers.get(tokenKey);
    log.info("using tenant {}", authToken);
    Context context = Context.current().withValue(GrpcContextKeys.TENANT_ID_KEY, authToken);
    return Contexts.interceptCall(context, call, headers, next);
  }
}
