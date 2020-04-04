package fr.pierrezemb.fdb.layer.etcd.store;

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

  private EtcdRecordStore recordStore;
  private FoundationDBContainer container = new FoundationDBContainer();
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
    EtcdRecord.KeyValue request = EtcdRecord.KeyValue
      .newBuilder()
      .setVersion(1)
      .setKey(key)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request);
    EtcdRecord.KeyValue storedRecord = recordStore.get(key.toByteArray());
    assertNotNull("storedRecord is null :(", storedRecord);
    assertEquals("stored request is different :(", request, storedRecord);

    // and a second
    ByteString key2 = ByteString.copyFromUtf8("/toto2");
    EtcdRecord.KeyValue request2 = EtcdRecord.KeyValue
      .newBuilder()
      .setVersion(1)
      .setKey(key2)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request2);
    EtcdRecord.KeyValue storedRecord2 = recordStore.get(key.toByteArray());
    assertNotNull("storedRecord is null :(", storedRecord2);
    assertEquals("stored request is different :(", request, storedRecord2);

    // and scan!
    List<EtcdRecord.KeyValue> scanResult = recordStore.scan("/tot".getBytes(), "/u".getBytes(), 1);
    assertEquals(2, scanResult.size());

    // and delete
    recordStore.delete("/tot".getBytes(), "/u".getBytes());
    List<EtcdRecord.KeyValue> scanResult2 = recordStore.scan("/tot".getBytes(), "/u".getBytes(), 1);
    assertEquals(0, scanResult2.size());
  }

  @AfterAll
  void tearsDown() {
    container.stop();
    clusterFile.delete();
  }
}
