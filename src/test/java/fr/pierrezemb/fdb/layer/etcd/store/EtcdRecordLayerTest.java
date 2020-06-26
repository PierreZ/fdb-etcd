package fr.pierrezemb.fdb.layer.etcd.store;

import com.google.protobuf.ByteString;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EtcdRecordLayerTest {

  private static final String TENANT = "my-tenant";
  private final FoundationDBContainer container = new FoundationDBContainer();
  private EtcdRecordLayer recordLayer;

  @BeforeAll
  void setUp() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    container.start();
    this.recordLayer = new EtcdRecordLayer(container.getClusterFile().getAbsolutePath());
  }

  @Test
  void crud() {
    // inserting an element
    ByteString key = ByteString.copyFromUtf8("/toto");
    ByteString value = ByteString.copyFromUtf8("tat");

    EtcdRecord.KeyValue request = EtcdRecord.KeyValue
      .newBuilder()
      .setKey(key)
      .setValue(value).build();
    recordLayer.put(TENANT, request, null);
    EtcdRecord.KeyValue storedRecord = recordLayer.get(TENANT, key.toByteArray());
    assertNotNull("storedRecord is null :(", storedRecord);
    assertArrayEquals("keys are different", key.toByteArray(), storedRecord.getKey().toByteArray());
    assertArrayEquals("values are different", value.toByteArray(), storedRecord.getValue().toByteArray());

    // and a second
    ByteString key2 = ByteString.copyFromUtf8("/toto2");
    EtcdRecord.KeyValue request2 = EtcdRecord.KeyValue
      .newBuilder()
      .setKey(key2)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordLayer.put(TENANT, request2, null);
    EtcdRecord.KeyValue storedRecord2 = recordLayer.get(TENANT, key2.toByteArray());
    assertArrayEquals("keys are different", key2.toByteArray(), storedRecord2.getKey().toByteArray());
    assertArrayEquals("values are different", value.toByteArray(), storedRecord2.getValue().toByteArray());

    // and scan!
    List<EtcdRecord.KeyValue> scanResult = recordLayer.scan(TENANT, "/tot".getBytes(), "/u".getBytes());
    assertEquals(2, scanResult.size());

    long count = recordLayer.stats(TENANT);
    assertEquals("count is bad", 2, count);

    // and delete
    recordLayer.delete(TENANT, "/tot".getBytes(), "/u".getBytes(), null);
    List<EtcdRecord.KeyValue> scanResult2 = recordLayer.scan(TENANT, "/tot".getBytes(), "/u".getBytes());
    assertEquals(0, scanResult2.size());

  }

  @AfterAll
  void tearsDown() {
    container.stop();
  }
}
