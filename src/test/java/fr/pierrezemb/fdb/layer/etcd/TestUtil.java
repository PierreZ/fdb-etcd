package fr.pierrezemb.fdb.layer.etcd;

import io.etcd.jetcd.ByteSequence;
import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestUtil {

  public static ByteSequence bytesOf(final String string) {
    return ByteSequence.from(string, UTF_8);
  }

  public static ByteString byteStringOf(final String string) {
    return ByteString.copyFrom(string.getBytes(UTF_8));
  }

  public static String randomString() {
    return java.util.UUID.randomUUID().toString();
  }

  public static ByteSequence randomByteSequence() {
    return ByteSequence.from(randomString(), Charsets.UTF_8);
  }

  public static int findNextAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public static void closeQuietly(final Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (final IOException ioe) {
      // ignore
    }
  }

  public interface TestCondition {
    boolean conditionMet();
  }

  public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs,
                                      String conditionDetails) throws InterruptedException {
    final long startTime = System.currentTimeMillis();
    boolean testConditionMet = false;
    while (!(testConditionMet = testCondition.conditionMet()) && ((System.currentTimeMillis() - startTime) < maxWaitMs)) {
      Thread.sleep(Math.min(maxWaitMs, 500L));
    }
    if (!testConditionMet) {
      conditionDetails = conditionDetails != null ? conditionDetails : "";
      throw new AssertionError("Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
    }
  }
}
