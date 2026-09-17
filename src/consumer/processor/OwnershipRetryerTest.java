package co.atoms.splitter.consumer.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.consumer.processor.exceptions.GrpcExceptions;
import co.atoms.splitter.testing.Gate;
import co.atoms.splitter.testing.MutableClock;
import co.atoms.splitter.testing.TestStreamObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OwnershipRetryerTest {

  private MutableClock clock;

  @BeforeEach
  public void setUp() {
    clock = MutableClock.at(Instant.now());
  }

  @Test
  public void passFirsTime() {
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(1), Duration.ofMillis(10), "test")
            .withClock(clock)
            .build();
    var o = new TestStreamObserver<Boolean>();
    retryer.retry(
        observer -> {
          observer.onNext(true);
          observer.onCompleted();
        },
        o);
    assertTrue(o.result.get());
    assertTrue(o.completed.get());
    assertNull(o.exception.get());
  }

  @Test
  public void passAfterMultipleCalls() {
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(5), Duration.ofMillis(10), "test")
            .withClock(clock)
            .withSleeper(d -> {})
            .withExceptionFilter(e -> true)
            .withLimit(5)
            .build();

    var o = new TestStreamObserver<Boolean>();
    var count = new AtomicInteger(0);
    retryer.retry(
        observer -> {
          if (count.incrementAndGet() < 3) {
            observer.onError(new RuntimeException("test"));
            return;
          }
          if (count.incrementAndGet() < 5) {
            observer.onError(GrpcExceptions.outOfRange("out of range"));
            return;
          }
          observer.onNext(true);
          observer.onCompleted();
        },
        o);
    assertTrue(o.result.get());
    assertTrue(o.completed.get());
    assertNull(o.exception.get());
  }

  @Test
  public void failsAfterMultipleCalls() {
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(1), Duration.ofMillis(10), "test")
            .withClock(clock)
            .withSleeper(d -> {})
            .withExceptionFilter(e -> true)
            .withLimit(5)
            .build();

    var o = new TestStreamObserver<Boolean>();
    retryer.retry(
        observer -> {
          observer.onError(new RuntimeException("test"));
        },
        o);
    assertNull(o.result.get());
    assertFalse(o.completed.get());
    assertEquals(
        o.exception.get().getMessage(),
        "DEADLINE_EXCEEDED: test did not succeed after 5 attempt(s)");
  }

  @Test
  public void failsWithDeadline() {
    var clock = MutableClock.at(Instant.now());
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(1), Duration.ofMillis(10), "test")
            .withExceptionFilter(e -> true)
            .withClock(clock)
            .build();
    var gate = new Gate();
    var started = new Gate();
    var finished = new Gate();

    var o = new TestStreamObserver<Boolean>();
    var t =
        new Thread(
            () -> {
              retryer.retry(
                  observer -> {
                    started.open();
                    gate.await();
                    observer.onError(new RuntimeException("test"));
                  },
                  o);
              finished.open();
            });
    t.start();

    started.await();
    clock.advance(Duration.ofSeconds(2));
    gate.open();
    finished.await();

    assertNull(o.result.get());
    assertFalse(o.completed.get());
    assertEquals(
        o.exception.get().getMessage(), "DEADLINE_EXCEEDED: test did not succeed within PT1S");
  }

  @Test
  public void passWithOutOfRange() {
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(1), Duration.ofMillis(10), "test")
            .withClock(clock)
            .withSleeper(d -> {})
            .withLimit(5)
            .build();

    var called = new AtomicBoolean(false);
    var o = new TestStreamObserver<Boolean>();
    retryer.retry(
        observer -> {
          if (!called.getAndSet(true)) {
            observer.onError(GrpcExceptions.outOfRange("test"));
          } else {
            observer.onNext(true);
            observer.onCompleted();
          }
        },
        o);
    assertTrue(o.result.get());
    assertTrue(o.completed.get());
    assertNull(o.exception.get());
  }

  @Test
  public void filterException() {
    var retryer =
        new OwnershipRetryer.Builder(Duration.ofSeconds(1), Duration.ofMillis(10), "test")
            .withClock(clock)
            .withSleeper(d -> {})
            .withExceptionFilter(e -> e instanceof IllegalArgumentException)
            .withLimit(5)
            .build();

    var o = new TestStreamObserver<Boolean>();
    var count = new AtomicInteger(0);
    retryer.retry(
        observer -> {
          if (count.incrementAndGet() < 2) {
            observer.onError(new IllegalArgumentException("test"));
            return;
          }
          observer.onError(new RuntimeException("test"));
        },
        o);
    assertNull(o.result.get());
    assertFalse(o.completed.get());
    assertEquals(o.exception.get().getMessage(), "test");
  }
}
