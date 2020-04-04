package fr.pierrezemb.fdb.layer.etcd;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class FoundationDBContainer extends GenericContainer<FoundationDBContainer> {
  public static final int FDB_PORT = 4500;
  private static final String FDB_VERSION = "6.2.19";
  private static final String FDB_IMAGE = "foundationdb/foundationdb";
  private static Logger log = LoggerFactory.getLogger(FoundationDBContainer.class);
  private File clusterFile;

  public FoundationDBContainer() {
    this(FDB_VERSION);
  }

  public FoundationDBContainer(String fdbVersion) {
    super(FDB_IMAGE + ":" + fdbVersion);
    withExposedPorts(FDB_PORT);
    waitingFor(Wait.forListeningPort());
  }

  @Override
  protected void containerIsStarted(InspectContainerResponse containerInfo) {
    try {
      Container.ExecResult initResult = execInContainer("fdbcli", "--exec", "configure new single memory");
      String stdout = initResult.getStdout();
      log.debug("init FDB stdout: " + stdout);
      int exitCode = initResult.getExitCode();
      log.debug("init FDB exit code: " + exitCode);

      boolean fdbReady = false;
      log.info("waiting for FDB to be healthy");

      // waiting for fdb to be up and healthy
      while (!fdbReady) {

        Container.ExecResult statusResult = execInContainer("fdbcli", "--exec", "status");
        stdout = statusResult.getStdout();

        if (stdout.contains("Healthy")) {
          fdbReady = true;
          log.info("fdb is healthy");
        } else {
          log.debug("fdb is unhealthy");
          Thread.sleep(10 * 1000);
        }
      }
      clusterFile = File.createTempFile("fdb", ".cluster");
      copyFileFromContainer("/var/fdb/fdb.cluster", clusterFile.getAbsolutePath());
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public File getClusterFile() {
    return clusterFile;
  }

}
