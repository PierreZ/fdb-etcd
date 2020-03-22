package fr.pierrezemb.fdb.layer.etcd.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.FDBTestBase;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EtcdRecordStoreTest extends FDBTestBase {

  private EtcdRecordStore recordStore;

  @BeforeAll
  void setUp() throws IOException, InterruptedException {
    super.internalSetup();
    recordStore = new EtcdRecordStore(clusterFilePath);
  }

  @Test
  void crud() {
    // inserting an element
    ByteString key = ByteString.copyFromUtf8("/toto");
    EtcdRecord.PutRequest request = EtcdRecord.PutRequest
      .newBuilder()
      .setKey(key)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request);
    EtcdRecord.PutRequest storedRecord = recordStore.get(Tuple.from(key.toByteArray()));
    assertNotNull("storedRecord is null :(", storedRecord);
    assertEquals("stored request is different :(", request, storedRecord);

    // and a second
    ByteString key2 = ByteString.copyFromUtf8("/toto2");
    EtcdRecord.PutRequest request2 = EtcdRecord.PutRequest
      .newBuilder()
      .setKey(key2)
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request2);
    EtcdRecord.PutRequest storedRecord2 = recordStore.get(Tuple.from(key.toByteArray()));
    assertNotNull("storedRecord is null :(", storedRecord2);
    assertEquals("stored request is different :(", request, storedRecord2);

    // and scan!
    List<EtcdRecord.PutRequest> scanResult = recordStore.scan(Tuple.from("/tot".getBytes()), Tuple.from("/u".getBytes()));
    assertEquals(2, scanResult.size());
  }

  @AfterAll
  void tearsDown() {
    super.internalShutdown();
  }
}
