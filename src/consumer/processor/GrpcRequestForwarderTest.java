package co.atoms.splitter.consumer.processor;

import static co.atoms.splitter.model.GrantState.ACTIVE;
import static co.atoms.splitter.model.GrantState.ALLOCATED_LOADED;
import static co.atoms.splitter.model.GrantState.REVOKED;
import static co.atoms.splitter.model.GrantState.REVOKED_UNLOADED;
import static co.atoms.splitter.testing.Asserts.assertCondition;
import static co.atoms.splitter.testing.Fixtures.DOMAIN1;
import static co.atoms.splitter.testing.Fixtures.DOMAIN2;
import static co.atoms.splitter.testing.Fixtures.EXP;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_5c;
import static co.atoms.splitter.testing.Fixtures.TS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.cluster.GrantInfo;
import co.atoms.splitter.consumer.processor.testing.TestRange;
import co.atoms.splitter.consumer.testing.TestConsumer;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.testing.Fixtures;
import co.atoms.splitter.testing.Gate;
import co.atoms.splitter.testing.MoreMessages;
import co.atoms.splitter.testing.MutableClock;
import co.atoms.splitter.testing.TestStreamObserver;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class GrpcRequestForwarderTest {

  private MutableClock clock;
  private OwnershipRetryer retrier;

  @BeforeEach
  public void setUp() {
    clock = MutableClock.at(Instant.now());
    retrier =
        new OwnershipRetryer.Builder(
                Duration.ofSeconds(10), Duration.ofMillis(100), "test range request")
            .withClock(clock)
            .build();
  }

  @Test
  public void handleNoMatch(TestInfo testInfo) {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);

    var consumer = TestConsumer.create(testInfo.getDisplayName(), DOMAIN1.service(), p);

    var forwarder =
        new GrpcRequestForwarder<>(
            consumer,
            p,
            managedChannel -> new TestStub(managedChannel, CallOptions.DEFAULT),
            TestStub::handle,
            retrier);

    // No owner found when no ranges are added and cluster is empty
    var key = Fixtures.newKey(DOMAIN1, "1");
    assertNotForwarded(o -> forwarder.forward(key, key, o));

    consumer.stopAsync();
  }

  @Test
  public void handleLocal(TestInfo testInfo) {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);

    var consumer =
        TestConsumer.create(
            Fixtures.createClock(), testInfo.getDisplayName(), DOMAIN1.service(), p);

    consumer.initialize(EXP);

    // Assign a grant
    var g1 = Grant.create("g1", SHARD_D2_GLOBAL_5c, ALLOCATED_LOADED, EXP, TS);
    consumer.assign(g1);
    consumer.notifyGrant(Grant.create("g1_old", SHARD_D2_GLOBAL_5c, REVOKED_UNLOADED, EXP, TS), g1);

    r.initialize.await();
    r.initialized.open();

    var key5 = Fixtures.newKey(DOMAIN2, "5");
    assertCondition(() -> p.lookup(key5, ALLOCATED_LOADED).isPresent());

    var forwarder =
        new GrpcRequestForwarder<>(
            consumer,
            p,
            managedChannel -> new TestStub(managedChannel, CallOptions.DEFAULT),
            TestStub::handle,
            retrier);

    // No owner found when the key is not in the range
    var key1 = Fixtures.newKey(DOMAIN2, "1");
    assertNotForwarded(o -> forwarder.forward(key1, key1, o));

    // No owner found when the state does not match
    assertNotForwarded(
        o ->
            forwarder.forward(
                key5, key5, o, new GrantState[] {ALLOCATED_LOADED}, new GrantState[] {REVOKED}));

    // Owner found when the key is in the range
    assertForwarded(o -> forwarder.forward(key5, key5, o), key5);

    // Owner found when the key is in the range and state matches
    assertForwarded(
        o ->
            forwarder.forward(
                key5,
                key5,
                o,
                new GrantState[] {ALLOCATED_LOADED},
                new GrantState[] {ALLOCATED_LOADED}),
        key5);

    consumer.stopAsync();
  }

  @Test
  public void handleRemote(TestInfo testInfo) {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);

    var consumer = TestConsumer.create(testInfo.getDisplayName(), DOMAIN1.service(), p);

    consumer.initialize(EXP);

    // Send cluster update
    var instance = Fixtures.newInstance(testInfo.getDisplayName());
    var g1 = GrantInfo.create("g1", SHARD_D2_GLOBAL_5c, ALLOCATED_LOADED);
    consumer.sendClusterSnapshot(MoreMessages.newAssignment(instance, g1));

    var forwarder =
        new GrpcRequestForwarder<>(
            consumer,
            p,
            managedChannel -> new TestStub(managedChannel, CallOptions.DEFAULT),
            TestStub::handle,
            retrier);

    // No owner found when the key is not owned by consumers in the cluster
    var key1 = Fixtures.newKey(DOMAIN2, "1");
    assertNotForwarded(o -> forwarder.forward(key1, key1, o));

    var key5 = Fixtures.newKey(DOMAIN2, "5");

    // No owner found when state does not match
    assertNotForwarded(
        o ->
            forwarder.forward(
                key5, key5, o, new GrantState[] {REVOKED}, new GrantState[] {ALLOCATED_LOADED}));

    // Owner found when the key is owned by a consumer in the cluster
    assertForwarded(o -> forwarder.forward(key5, key5, o), key5);

    // Owner found when the key is owned by a consumer in the cluster and state matches
    assertForwarded(
        o ->
            forwarder.forward(
                key5,
                key5,
                o,
                new GrantState[] {ALLOCATED_LOADED},
                new GrantState[] {ALLOCATED_LOADED}),
        key5);

    consumer.stopAsync();
  }

  @Test
  public void handleWithActiveLocal(TestInfo testInfo) {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);

    var consumer =
        TestConsumer.create(
            Fixtures.createClock(), testInfo.getDisplayName(), DOMAIN1.service(), p);

    consumer.initialize(EXP);

    var instance = Fixtures.newInstance(testInfo.getDisplayName());

    // Assign a grant
    var g1 = Grant.create("g1", SHARD_D2_GLOBAL_5c, ACTIVE, EXP, TS);
    consumer.assign(g1);

    r.initialize.await();
    r.initialized.open();
    r.activated.open();

    // Send cluster update
    var g1New = GrantInfo.create("g1_new", SHARD_D2_GLOBAL_5c, ALLOCATED_LOADED);
    consumer.sendClusterSnapshot(MoreMessages.newAssignment(instance, g1New));

    var forwarder =
        new GrpcRequestForwarder<>(
            consumer,
            p,
            managedChannel -> new TestStub(managedChannel, CallOptions.DEFAULT),
            TestStub::handle,
            retrier);

    // No owner found when the key is not owned by consumers in the cluster
    var key1 = Fixtures.newKey(DOMAIN2, "1");
    assertNotForwarded(o -> forwarder.forward(key1, key1, o));

    // Local owner is found for the active grant
    var key5 = Fixtures.newKey(DOMAIN2, "5");
    assertForwarded(o -> forwarder.forward(key5, key5, o), key5);

    consumer.stopAsync();
  }

  @Test
  public void handleRetry(TestInfo testInfo) {
    var r = new TestRange();
    var p = new TestWorkProcessor((id, shard, ownership) -> r);

    var consumer = TestConsumer.create(testInfo.getDisplayName(), DOMAIN1.service(), p);

    consumer.initialize(EXP);

    var retrier =
        new OwnershipRetryer.Builder(
                Duration.ofSeconds(10), Duration.ofMillis(100), "test range request")
            .withClock(clock)
            .build();

    var forwarder =
        new GrpcRequestForwarder<>(
            consumer,
            p,
            managedChannel -> new TestStub(managedChannel, CallOptions.DEFAULT),
            TestStub::handle,
            retrier);

    // No owner found when the key is not owned by consumers in the cluster
    var key5 = Fixtures.newKey(DOMAIN2, "5");

    // No owner found when state does not match
    var resp = new TestStreamObserver<QualifiedDomainKey>();

    var started = new Gate();
    var finished = new Gate();
    var count = clock.getCount();
    var t =
        new Thread(
            () -> {
              started.open();
              forwarder.forward(key5, key5, resp);
              finished.open();
            });
    t.start();

    started.await();
    // Wait for retrier to retry
    assertCondition(() -> clock.getCount() > count + 4);

    // Send cluster update
    var instance = Fixtures.newInstance(testInfo.getDisplayName());
    var g1 = GrantInfo.create("g1", SHARD_D2_GLOBAL_5c, ALLOCATED_LOADED);
    consumer.sendClusterSnapshot(MoreMessages.newAssignment(instance, g1));

    finished.await();

    assertNull(resp.exception.get());
    assertEquals(resp.result.get(), key5);
    assertTrue(resp.completed.get());

    consumer.stopAsync();
  }

  private void assertNotForwarded(Consumer<StreamObserver<QualifiedDomainKey>> action) {
    var resp = new TestStreamObserver<QualifiedDomainKey>();

    runAsync(action, resp);

    assertEquals(
        resp.exception.get().getMessage(),
        "DEADLINE_EXCEEDED: test range request did not succeed within PT10S");
    assertNull(resp.result.get());
    assertFalse(resp.completed.get());
  }

  public void assertForwarded(
      Consumer<StreamObserver<QualifiedDomainKey>> action, QualifiedDomainKey key) {
    var resp = new TestStreamObserver<QualifiedDomainKey>();

    // Owner found when the key is in the range
    resp = new TestStreamObserver<>();

    runAsync(action, resp);

    assertNull(resp.exception.get());
    assertEquals(resp.result.get(), key);
    assertTrue(resp.completed.get());
  }

  private void runAsync(
      Consumer<StreamObserver<QualifiedDomainKey>> action,
      TestStreamObserver<QualifiedDomainKey> observer) {
    var started = new Gate();
    var finished = new Gate();
    var count = clock.getCount();
    var t =
        new Thread(
            () -> {
              started.open();
              action.accept(observer);
              finished.open();
            });
    t.start();

    started.await();
    assertCondition(() -> clock.getCount() > count + 2 || observer.completed.get());
    clock.advance(Duration.ofSeconds(10));
    finished.await();
  }

  private static class TestWorkProcessor extends LoadingWorkProcessor<TestRange> {
    public TestWorkProcessor(Range.Factory<TestRange> rangeFactory) {
      super(rangeFactory);
    }
  }

  private static class TestStub extends AbstractBlockingStub<TestStub> {
    protected TestStub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected TestStub build(Channel channel, CallOptions callOptions) {
      return new TestStub(channel, callOptions);
    }

    public QualifiedDomainKey handle(QualifiedDomainKey key) {
      return key;
    }
  }
}
