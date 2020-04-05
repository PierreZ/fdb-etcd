package fr.pierrezemb.fdb.layer.etcd.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.FoundationDBContainer;
import java.io.File;
import java.io.IOException;
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
      .setKey(key)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request);
    EtcdRecordStore.Result storedRecord = recordStore.get(key.toByteArray());
    assertNotNull("storedRecord is null :(", storedRecord.getRecords());
    assertArrayEquals("stored request key is different :(", request.getKey().toByteArray(), storedRecord.getRecords().get(0).getKey().toByteArray());
    assertArrayEquals("stored request value is different :(", request.getValue().toByteArray(), storedRecord.getRecords().get(0).getValue().toByteArray());

    // and a second
    ByteString key2 = ByteString.copyFromUtf8("/toto2");
    EtcdRecord.KeyValue request2 = EtcdRecord.KeyValue
      .newBuilder()
      .setKey(key2)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request2);
    EtcdRecordStore.Result storedRecord2 = recordStore.get(key2.toByteArray());
    assertArrayEquals("stored request key is different :(", request2.getKey().toByteArray(), storedRecord2.getRecords().get(0).getKey().toByteArray());
    assertArrayEquals("stored request value is different :(", request2.getValue().toByteArray(), storedRecord2.getRecords().get(0).getValue().toByteArray());
    assertTrue("readVersion is bad", storedRecord.getReadVersion() < storedRecord2.getReadVersion());

    // and scan!
    EtcdRecordStore.Result scanResult = recordStore.scan("/tot".getBytes(), "/u".getBytes());
    assertEquals(2, scanResult.getRecords().size());
    assertTrue("readVersion is bad", storedRecord2.getReadVersion() < scanResult.getReadVersion());

    // and delete
    recordStore.delete("/tot".getBytes(), "/u".getBytes());
    EtcdRecordStore.Result scanResult2 = recordStore.scan("/tot".getBytes(), "/u".getBytes());
    assertEquals(0, scanResult2.getRecords().size());
    assertTrue("readVersion is bad", scanResult.getReadVersion() < scanResult.getReadVersion());
  }

  @AfterAll
  void tearsDown() {
    container.stop();
    clusterFile.delete();
  }
}
