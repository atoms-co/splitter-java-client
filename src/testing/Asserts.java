package co.atoms.splitter.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Assertions;

public class Asserts {
  private Asserts() {}

  public static <T> void assertElement(LinkedBlockingQueue<T> queue, T expected) {
    assertElement(queue, expected, null);
  }

  public static <T> void assertElement(
      LinkedBlockingQueue<T> queue, T expected, @Nullable String message) {
    Assertions.assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          var e = queue.poll(10, TimeUnit.SECONDS);
          assertNotNull(e, "empty");
          assertEquals(e, expected, message);
        });
  }

  public static <T> T assertElement(LinkedBlockingQueue<T> queue) {
    AtomicReference<T> element = new AtomicReference<>();
    Assertions.assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          var e = queue.poll(10, TimeUnit.SECONDS);
          element.set(e);
        });
    var e = element.get();
    assertNotNull(e, "empty");
    return e;
  }

  public static <T> void assertElementPresent(LinkedBlockingQueue<T> queue) {
    assertElementPresent(queue, null);
  }

  public static <T> void assertElementPresent(
      LinkedBlockingQueue<T> queue, @Nullable String message) {
    Assertions.assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          var e = queue.poll(10, TimeUnit.SECONDS);
          assertNotNull(e, message);
        });
  }

  public static <T> void assertCondition(Callable<Boolean> condition) {
    Assertions.assertTimeout(
        Duration.ofSeconds(10),
        () -> {
          while (!condition.call()) {
            Thread.sleep(100);
          }
        });
  }

  public static void assertProtosEqual(Message expected, Message actual, String... message) {
    assertTrue(message.length <= 1, "Only one message is supported");
    if (message.length == 1) {
      assertEquals(expected.toString(), actual.toString(), message[0]);
    } else {
      assertEquals(expected.toString(), actual.toString());
    }
  }
}
