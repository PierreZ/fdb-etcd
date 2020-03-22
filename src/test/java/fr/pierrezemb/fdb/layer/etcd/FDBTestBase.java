package fr.pierrezemb.fdb.layer.etcd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public abstract class FDBTestBase {

  private GenericContainer fdb = new GenericContainer("foundationdb/foundationdb:6.2.19")
    .withExposedPorts(4500)
    .waitingFor(Wait.forListeningPort());
  public String clusterFilePath;

  protected final void internalSetup() throws InterruptedException, IOException {
    fdb.start();
    fdb.getLogs();

    Thread.sleep(3 * 1000);

    //Container.ExecResult initResult = fdb.execInContainer("fdbcli", "--exec", "\"configure new single memory ; status\"");
    Container.ExecResult initResult = fdb.execInContainer("fdbcli", "--exec", "configure new single memory");
    String stdout = initResult.getStdout();
    System.out.println(stdout);
    int exitCode = initResult.getExitCode();
    System.out.println(exitCode);

    boolean fdbReady = false;

    // waiting for fdb to be up and healthy
    while (!fdbReady) {

      Container.ExecResult statusResult = fdb.execInContainer("fdbcli", "--exec", "status");
      stdout = statusResult.getStdout();

      if (stdout.contains("Healthy")) {
        fdbReady = true;
        System.out.println("fdb is healthy");
      } else {
        System.out.println("fdb is unhealthy");
        Thread.sleep(10 * 1000);
      }
    }

    // handle fdb cluster file
    Path path = Files.createTempDirectory("java-fdb-etcd-tests");
    clusterFilePath = path.toAbsolutePath().toString() + "/fdb.cluster";
    System.out.println(clusterFilePath);
    fdb.copyFileFromContainer("/var/fdb/fdb.cluster", clusterFilePath);
  }

  protected void internalShutdown() {
    System.out.println("shutting down fdb");
    this.fdb.stop();
  }
}
