package fr.pierrezemb.fdb.layer.etcd.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.ByteString;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EtcdRecordStoreTest {

  private final FoundationDBContainer container = new FoundationDBContainer();
  private EtcdRecordStore recordStore;
  private File clusterFile;

  @BeforeAll
  void setUp() throws IOException {
    container.start();
    clusterFile = container.getClusterFile();
    recordStore = new EtcdRecordStore(clusterFile.getPath());
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
    recordStore.put(request);
    EtcdRecord.KeyValue storedRecord = recordStore.get(key.toByteArray());
    assertNotNull("storedRecord is null :(", storedRecord);
    assertArrayEquals("keys are different", key.toByteArray(), storedRecord.getKey().toByteArray());
    assertArrayEquals("values are different", value.toByteArray(), storedRecord.getValue().toByteArray());

    // and a second
    ByteString key2 = ByteString.copyFromUtf8("/toto2");
    EtcdRecord.KeyValue request2 = EtcdRecord.KeyValue
      .newBuilder()
      .setKey(key2)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request2);
    EtcdRecord.KeyValue storedRecord2 = recordStore.get(key2.toByteArray());
    assertArrayEquals("keys are different", key2.toByteArray(), storedRecord2.getKey().toByteArray());
    assertArrayEquals("values are different", value.toByteArray(), storedRecord2.getValue().toByteArray());

    // and scan!
    List<EtcdRecord.KeyValue> scanResult = recordStore.scan("/tot".getBytes(), "/u".getBytes());
    assertEquals(2, scanResult.size());

    // and delete
    recordStore.delete("/tot".getBytes(), "/u".getBytes());
    List<EtcdRecord.KeyValue> scanResult2 = recordStore.scan("/tot".getBytes(), "/u".getBytes());
    assertEquals(0, scanResult2.size());

    long count = recordStore.stats();
    assertEquals("count is bad", 2, count);
  }

  @AfterAll
  void tearsDown() {
    container.stop();
    clusterFile.delete();
  }
}
