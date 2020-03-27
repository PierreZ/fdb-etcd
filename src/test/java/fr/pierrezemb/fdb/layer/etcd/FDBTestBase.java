package fr.pierrezemb.fdb.layer.etcd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

public class FDBTestBase {

  private final Logger log = LoggerFactory.getLogger(FDBTestBase.class);

  private GenericContainer fdb = new GenericContainer("foundationdb/foundationdb:6.2.19")
    .withExposedPorts(4500)
    .withLogConsumer(new Slf4jLogConsumer(log))
    .waitingFor(Wait.forListeningPort());

  public String clusterFilePath = "./fdb.cluster";
  private Path file;

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
    createClusterFile(fdb.getContainerIpAddress(), fdb.getFirstMappedPort());
    System.out.println(clusterFilePath);
    fdb.copyFileFromContainer("/var/fdb/fdb.cluster", clusterFilePath);
  }

  protected void createClusterFile(String ip, Integer port) throws IOException {
    file = Paths.get(clusterFilePath);
    String content = "docker:docker@" + ip + ":" + port;
    Files.write(file, content.getBytes());
  }

  protected void internalShutdown() {
    System.out.println("shutting down fdb");
    this.fdb.stop();
  }
}
