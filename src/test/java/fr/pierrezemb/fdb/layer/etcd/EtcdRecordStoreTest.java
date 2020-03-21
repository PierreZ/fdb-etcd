package fr.pierrezemb.fdb.layer.etcd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EtcdRecordStoreTest {

  private GenericContainer fdb = new GenericContainer("foundationdb/foundationdb:6.2.19")
    .withExposedPorts(4500)
    .waitingFor(Wait.forListeningPort());
  private String clusterFilePath;

  @BeforeEach
  void setUp() throws IOException {
    fdb.start();
    fdb.getLogs();

    Path path = Files.createTempDirectory("java-fdb-etcd-tests");
    clusterFilePath = path.toAbsolutePath().toString() + "/fdb.cluster";
    System.out.println(clusterFilePath);
    fdb.copyFileFromContainer("/var/fdb/fdb.cluster", clusterFilePath);
  }

  @Test
  public void testMetaStoreWithoutError() {
    EtcdRecordStore recordStore = new EtcdRecordStore(clusterFilePath);
  }
}
