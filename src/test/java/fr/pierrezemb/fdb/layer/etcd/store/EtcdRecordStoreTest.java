package fr.pierrezemb.fdb.layer.etcd.store;

import com.google.protobuf.ByteString;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.FDBTestBase;
import java.io.IOException;
import org.junit.Before;
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
  void put() {
    EtcdRecord.PutRequest request = EtcdRecord.PutRequest
      .newBuilder()
      .setKey(ByteString.copyFromUtf8("/toto"))
      .setValue(ByteString.copyFromUtf8("tat")).build();
    recordStore.put(request);
  }

  @AfterAll
  void tearsDown() {
    super.internalShutdown();
  }
}
