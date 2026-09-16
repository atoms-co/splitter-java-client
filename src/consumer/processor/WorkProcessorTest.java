package co.atoms.splitter.consumer.processor;

import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_15;
import static org.junit.jupiter.api.Assertions.*;

import co.atoms.splitter.consumer.processor.testing.TestRange;
import co.atoms.splitter.consumer.testing.TestOwnership;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.testing.MutableClock;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class WorkProcessorTest {

  ExecutorService executor = Executors.newSingleThreadExecutor();

  @Test
  public void releasedWhileWaitForActive() throws Exception {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);
    var o = new TestOwnership(Instant.now());

    var result = executor.submit(() -> p.handleWork("g1", SHARD_D2_GLOBAL_15, o));

    o.active.set(false);
    r.initialize.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.ALLOCATED)
            .isPresent());
    r.initialized.open();

    o.waitingForActive.await();
    o.waitForActive.open();
    r.terminate.await();

    assertTrue(o.onTermination.get());

    r.terminated.open();
    result.get(10, TimeUnit.SECONDS);
    assertFalse(p.getRanges().lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15)).isPresent());
  }

  @Test
  public void releasedWhileWaitForRevoked() throws Exception {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);
    var o = new TestOwnership(Instant.now());

    var result = executor.submit(() -> p.handleWork("g1", SHARD_D2_GLOBAL_15, o));

    o.active.set(true);
    o.waitForActive.open();
    o.revoked.set(false);

    r.initialize.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.ALLOCATED)
            .isPresent());
    r.initialized.open();
    r.activate.await();
    r.activated.open();

    o.waitingForRevoked.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.ACTIVE)
            .isPresent());
    o.waitForRevoked.open();

    r.terminate.await();
    assertTrue(o.onTermination.get());

    r.terminated.open();
    result.get(10, TimeUnit.SECONDS);
    assertFalse(p.getRanges().lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15)).isPresent());
  }

  @Test
  public void normalFlow() throws Exception {
    var r = new TestRange();
    Clock clock = MutableClock.at(ZoneId.systemDefault(), Instant.parse("2021-01-01T11:22:33Z"));

    var p = new TestWorkProcessor(clock, (id, shard, ownership) -> r);
    var o = new TestOwnership(clock.instant().plus(Duration.ofSeconds(10)));

    var result = executor.submit(() -> p.handleWork("g1", SHARD_D2_GLOBAL_15, o));

    o.active.set(true);
    o.revoked.set(true);
    o.waitForActive.open();

    r.initialize.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.ALLOCATED)
            .isPresent());
    r.initialized.open();

    r.activate.await();
    r.activated.open();

    o.waitingForRevoked.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.ACTIVE)
            .isPresent());
    o.waitForRevoked.open();

    r.drain.await();
    assertTrue(
        p.getRanges()
            .lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15), GrantState.REVOKED)
            .isPresent());
    r.drained.open();

    r.terminate.await();
    assertTrue(o.onTermination.get());
    assertEquals(Duration.ofSeconds(10), r.timeout.get());

    r.terminated.open();
    result.get(10, TimeUnit.SECONDS);
    assertFalse(p.getRanges().lookup(createQualifiedDomainKey(SHARD_D2_GLOBAL_15)).isPresent());
  }

  private static class TestWorkProcessor extends WorkProcessor<TestRange> {
    public TestWorkProcessor(Range.Factory<TestRange> rangeFactory) {
      super(rangeFactory);
    }

    public TestWorkProcessor(Clock clock, Range.Factory<TestRange> rangeFactory) {
      super(clock, rangeFactory);
    }
  }

  private QualifiedDomainKey createQualifiedDomainKey(Shard shard) {
    return QualifiedDomainKey.create(shard.domain(), shard.from());
  }
}
