package fr.pierrezemb.fdb.layer.etcd.service;

import static fr.pierrezemb.fdb.layer.etcd.TestUtil.bytesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import fr.pierrezemb.fdb.layer.etcd.MainVerticle;
import io.etcd.jetcd.Auth;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.auth.AuthRoleListResponse;
import io.etcd.jetcd.auth.Permission;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * https://github.com/etcd-io/jetcd/blob/master/jetcd-core/src/test/java/io/etcd/jetcd/AuthClientTest.java
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuthServiceTest {

  private static final ByteSequence SAMPLE_KEY = ByteSequence.from("sample_key".getBytes());
  private static final ByteSequence SAMPLE_VALUE = ByteSequence.from("sample_value".getBytes());
  private static final ByteSequence SAMPLE_KEY_2 = ByteSequence.from("sample_key2".getBytes());
  private static final ByteSequence SAMPLE_VALUE_2 = ByteSequence.from("sample_value2".getBytes());
  private static final ByteSequence SAMPLE_KEY_3 = ByteSequence.from("sample_key3".getBytes());

  private static Auth authDisabledAuthClient;
  private static KV authDisabledKVClient;
  private final ByteSequence rootRoleKey = bytesOf("root");
  private final ByteSequence rootRoleValue = bytesOf("b");
  private final ByteSequence rootRoleKeyRangeBegin = bytesOf("root");
  private final ByteSequence rootRoleKeyRangeEnd = bytesOf("root1");
  private final ByteSequence userRoleKey = bytesOf("foo");
  private final ByteSequence userRoleValue = bytesOf("bar");
  private final ByteSequence userRoleKeyRangeBegin = bytesOf("foo");
  private final ByteSequence userRoleKeyRangeEnd = bytesOf("foo1");
  private final String rootString = "root";
  private final ByteSequence root = bytesOf(rootString);
  private final ByteSequence rootPass = bytesOf("123");
  private final String rootRoleString = "root";
  private final ByteSequence rootRole = bytesOf(rootRoleString);
  private final String userString = "user";
  private final ByteSequence user = bytesOf(userString);
  private final ByteSequence userPass = bytesOf("userPass");
  private final ByteSequence userNewPass = bytesOf("newUserPass");
  private final String userRoleString = "userRole";
  private final ByteSequence userRole = bytesOf(userRoleString);
  private FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("auth-enabled", true)
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void basicTestAuth() throws Exception {
    final Client rootClient = Client.builder()
      .endpoints("http://localhost:8080")
      .user(ByteSequence.from("root".getBytes()))
      .password(ByteSequence.from("roopasswd".getBytes())).build();

    KV kvClient = rootClient.getKVClient();
    CompletableFuture<PutResponse> feature = kvClient.put(SAMPLE_KEY, SAMPLE_VALUE);
    PutResponse response = feature.get();
    assertNotNull(response.getHeader());
    assertFalse(response.hasPrevKv());

    final Client anotherUserClient = Client.builder()
      .endpoints("http://localhost:8080")
      .user(ByteSequence.from("pierre".getBytes()))
      .password(ByteSequence.from("whatever".getBytes())).build();

    kvClient = anotherUserClient.getKVClient();
    GetResponse getResponse = kvClient.get(SAMPLE_KEY).get();
    assertNotNull(response.getHeader());
    assertEquals(0, getResponse.getCount());
  }

  @Test
  public void testAuth() throws Exception {
    Client client = Client.builder().endpoints("http://localhost:8080")
      .user(ByteSequence.from("root".getBytes()))
      .password(ByteSequence.from("roopasswd".getBytes())).build();

    authDisabledKVClient = client.getKVClient();
    authDisabledAuthClient = client.getAuthClient();

    authDisabledAuthClient.roleAdd(rootRole).get();
    authDisabledAuthClient.roleAdd(userRole).get();

    final AuthRoleListResponse response = authDisabledAuthClient.roleList().get();
    assertEquals(response.getRoles().get(0), userRoleString);

    authDisabledAuthClient
      .roleGrantPermission(rootRole, rootRoleKeyRangeBegin, rootRoleKeyRangeEnd, Permission.Type.READWRITE).get();
    authDisabledAuthClient.roleGrantPermission(userRole, userRoleKeyRangeBegin, userRoleKeyRangeEnd, Permission.Type.READWRITE).get();

    authDisabledAuthClient.userAdd(root, rootPass).get();
    authDisabledAuthClient.userAdd(user, userPass).get();

    authDisabledAuthClient.userChangePassword(user, userNewPass).get();

    List<String> users = authDisabledAuthClient.userList().get().getUsers();
    assertTrue(users.contains(rootString));
    assertTrue(users.contains(userString));

    authDisabledAuthClient.userGrantRole(root, rootRole).get();
    authDisabledAuthClient.userGrantRole(user, rootRole).get();
    authDisabledAuthClient.userGrantRole(user, userRole).get();

    assertEquals(authDisabledAuthClient.userGet(root).get().getRoles().get(0), rootRoleString);
    assertTrue(authDisabledAuthClient.userGet(root).get().getRoles().contains(rootRoleString));
    assertTrue(authDisabledAuthClient.userGet(root).get().getRoles().contains(userRoleString));
  }
}
