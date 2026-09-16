package co.atoms.splitter.testing;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** A gate that can be opened and waited on. Can be used to test asynchronous logic. */
public class Gate {
  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile boolean failed = false;

  public void open() {
    latch.countDown();
  }

  /** Wait for the gate to be opened. Fails the test if the gate is not opened within 10 seconds. */
  public void await() {
    try {
      failed = !latch.await(10, TimeUnit.SECONDS);
      assertFalse(failed);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** Check if the gate failed to open. Can be used to verify expected state. */
  public boolean failed() {
    return failed;
  }
}
