package co.atoms.splitter.consumer;

import static co.atoms.splitter.model.GrantState.ACTIVE;
import static co.atoms.splitter.model.GrantState.ALLOCATED;
import static co.atoms.splitter.model.GrantState.ALLOCATED_LOADED;
import static co.atoms.splitter.model.GrantState.REVOKED;
import static co.atoms.splitter.model.GrantState.REVOKED_UNLOADED;
import static co.atoms.splitter.testing.Fixtures.DOMAIN1;
import static co.atoms.splitter.testing.Fixtures.EXP;
import static co.atoms.splitter.testing.Fixtures.SHARD_D1_UNIT_0e;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_15;
import static co.atoms.splitter.testing.Fixtures.TS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

import co.atoms.splitter.cluster.Cluster;
import co.atoms.splitter.cluster.GrantInfo;
import co.atoms.splitter.consumer.testing.TestConsumer;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.testing.Fixtures;
import co.atoms.splitter.testing.Gate;
import co.atoms.splitter.testing.MoreMessages;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;

@Timeout(value = 60, threadMode = SEPARATE_THREAD)
@Isolated
public class ConsumerTest {

  @Test
  void clusterUpdates(TestInfo testInfo) {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Send initial cluster snapshot
    var g1 = GrantInfo.create("g1", SHARD_D1_UNIT_0e, GrantState.ALLOCATED_LOADED);
    deps.consumer.sendClusterSnapshot(
        List.of(SHARD_D1_UNIT_0e, SHARD_D2_GLOBAL_15),
        MoreMessages.newAssignment(deps.instance, g1));

    var cluster = deps.consumer.getCluster();
    assertEquals(cluster.getConsumers().size(), 1);
    assertEquals(
        cluster.getConsumerGrants(deps.instance.id()).get(),
        Cluster.ConsumerWithGrants.create(
            deps.instance,
            List.of(GrantInfo.create("g1", SHARD_D1_UNIT_0e, GrantState.ALLOCATED_LOADED))));

    // Send cluster update
    var g2 = GrantInfo.create("g2", SHARD_D2_GLOBAL_15, GrantState.ACTIVE);
    deps.consumer.sendClusterAssign(1, MoreMessages.newAssignment(deps.instance, g2));

    cluster = deps.consumer.getCluster();
    assertEquals(cluster.getConsumers().size(), 1);
    var grants =
        List.of(
            GrantInfo.create("g1", SHARD_D1_UNIT_0e, GrantState.ALLOCATED_LOADED),
            GrantInfo.create("g2", SHARD_D2_GLOBAL_15, GrantState.ACTIVE));
    var actual = cluster.getConsumerGrants(deps.instance.id()).get();
    var actualGrants =
        actual.grants().stream().sorted(Comparator.comparing(GrantInfo::id)).toList();
    assertEquals(actual.consumer(), deps.instance);
    assertEquals(actualGrants, grants);

    deps.stop();
  }

  @Test
  public void reconnect(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Assign grants
    var g1 = Grant.create("g1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    var g2 = Grant.create("g2", SHARD_D2_GLOBAL_15, ALLOCATED, EXP, TS);
    deps.assign(g1, g2);

    // Disconnect from the server
    deps.consumer.terminateConnection();

    // Session is initialized again
    deps.consumer.assertSentEstablish();
    deps.consumer.sendEstablished();
    // Assigned grants are sent in the register message
    deps.consumer.assertSentRegister(g1, g2);

    deps.stop();
  }

  @Test
  public void terminatedBeforeEstablished(TestInfo testInfo) {
    var deps = setupDeps(testInfo);

    deps.consumer.startAsync();
    deps.consumer.assertSentEstablish();

    // Terminate stream before established is sent
    deps.consumer.terminateConnection();

    // Session is connecting again
    deps.consumer.assertSentEstablish();
    deps.consumer.sendEstablished();
    deps.consumer.assertSentRegister();

    deps.stop();
  }

  @Test
  public void stopWhileStartingWithServerExtend(TestInfo testInfo) {
    var deps = setupDeps(testInfo);

    deps.consumer.startAsync();
    deps.consumer.assertSentEstablish();

    // Stop while the consumer is starting
    deps.consumer.stopAsync();

    // Establish the session
    deps.consumer.sendEstablished();
    deps.consumer.assertSentRegister();
    // If the consumer doesn't disconnect the work pool, this message will cause us to hang while
    // waiting for orderly stop.
    deps.consumer.extend(EXP);

    // Consumer must be terminated eventually
    deps.waitForOrderlyStop();
  }

  @Test
  public void stopWhileStartingWithoutServerExtend(TestInfo testInfo) {
    var deps = setupDeps(testInfo);

    deps.consumer.startAsync();
    deps.consumer.assertSentEstablish();

    // Stop while the consumer is starting
    deps.consumer.stopAsync();

    // Establish the session
    deps.consumer.sendEstablished();
    deps.consumer.assertSentRegister();

    // Consumer must be terminated eventually
    deps.waitForOrderlyStop();
  }

  @Test
  public void extend(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Assign a grant
    var g1 = Grant.create("g1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    AtomicReference<Ownership> ownership = new AtomicReference<>();
    deps.assign(
        (id, shard, o) -> {
          ownership.set(o);
          deps.grants.get(id).started.open();
          deps.grants.get(id).finish.await();
        },
        g1);
    assertEquals(ownership.get().getExpiration(), EXP);

    // Send a new lease
    var newLease = EXP.plus(Duration.ofSeconds(10));
    deps.consumer.extend(newLease);

    // Lease is updated
    assertEquals(ownership.get().getExpiration(), newLease);

    // Release the grant
    deps.grants.get("g1").finish.open();
    deps.consumer.assertSentReleased(Grant.create("g1", SHARD_D1_UNIT_0e, ALLOCATED, newLease, TS));

    deps.stop();
  }

  @Test
  public void promote(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Assign an allocated grant
    var g1 = Grant.create("g1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    Gate active = new Gate();
    deps.assign(
        (id, shard, o) -> {
          deps.grants.get(id).started.open();
          if (o.waitForActive()) {
            active.open();
          }
          deps.grants.get(id).finish.await();
        },
        g1);

    // Promote to active
    deps.consumer.promote(g1.withState(ACTIVE));

    active.await();

    // Release the grant
    deps.grants.get("g1").finish.open();
    deps.consumer.assertSentReleased(g1.withState(ACTIVE));

    deps.stop();
  }

  @Test
  public void revoke(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Assign an allocated grant
    var g1 = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(g1);

    // Revoke the grant
    var revoked = Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS);
    deps.consumer.revoke(revoked);
    deps.grants.get("gid1").revoked.await();

    // Release the grant
    deps.grants.get("gid1").finish.open();
    deps.consumer.assertSentReleased(revoked);

    deps.stop();
  }

  @Test
  public void notify(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo);

    deps.consumer.initialize(EXP);

    // Assign an allocated grant
    var g1 = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    Gate unloaded = new Gate();
    deps.assign(
        (id, shard, o) -> {
          deps.grants.get(id).started.open();
          if (o.waitForOtherGrantToUnload()) {
            o.load();
            unloaded.open();
          }
          deps.grants.get(id).finish.await();
        },
        g1);

    // Notify about unloading
    deps.consumer.notifyGrant(
        Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), g1);
    unloaded.await();

    // Grant is loaded
    var loadedGrant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS);
    deps.consumer.assertSentUpdate(loadedGrant);

    // Release the grant
    deps.grants.get("gid1").finish.open();
    deps.consumer.assertSentReleased(loadedGrant);

    deps.stop();
  }

  @Test
  public void awaitStartedWithTimeout(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo, Clock.systemDefaultZone());

    assertThrows(
        TimeoutException.class,
        () -> deps.consumer.awaitRunning(Duration.ofMillis(10)),
        "We haven't told the consumer to start so we expect an exception here");

    deps.consumer.initialize(EXP);

    deps.consumer.awaitRunning(Duration.ofMillis(10));
    deps.consumer.awaitRunning();

    deps.stop();
  }

  @Test
  public void startedAwaitTerminatedWithTimeout(TestInfo testInfo) throws Exception {
    var deps = setupDeps(testInfo, Clock.systemDefaultZone());

    assertThrows(
        TimeoutException.class,
        () -> deps.consumer.awaitTerminated(Duration.ofMillis(10)),
        "We haven't told the consumer to stop so we expect an exception here");

    deps.consumer.initialize(EXP);

    deps.consumer.stopAsync();

    deps.consumer.terminateConnection();
    deps.consumer.awaitTerminated(Duration.ofMillis(10));
    deps.consumer.awaitTerminated();
  }

  private static class GrantHandler implements WorkHandler {
    final WorkHandler workHandler;
    final Gate started = new Gate();
    final Gate revoked = new Gate();
    final Gate finish = new Gate();

    GrantHandler() {
      this.workHandler = this;
    }

    GrantHandler(WorkHandler workHandler) {
      this.workHandler = workHandler;
    }

    @Override
    public void handleWork(String id, Shard shard, Ownership ownership) {
      ownership.onTermination(finish::open);
      started.open();
      try {
        if (ownership.waitForRevoked()) {
          revoked.open();
        }
      } finally {
        finish.await();
      }
    }
  }

  private static class ConsumerTestDeps {
    final Instance instance;
    final TestConsumer consumer;

    final Map<String, GrantHandler> grants;

    ConsumerTestDeps(Instance instance, Map<String, GrantHandler> grants, TestConsumer consumer) {
      this.instance = instance;
      this.grants = grants;
      this.consumer = consumer;
    }

    void assign(Grant... grants) {
      for (var g : grants) {
        var h = new GrantHandler();
        this.grants.put(g.id(), h);
      }
      consumer.assign(grants);
      for (var g : grants) {
        var h = this.grants.get(g.id());
        h.started.await();
      }
    }

    void assign(WorkHandler handler, Grant... grants) {
      for (var g : grants) {
        var h = new GrantHandler(handler);
        this.grants.put(g.id(), h);
      }
      consumer.assign(grants);
      for (var g : grants) {
        var h = this.grants.get(g.id());
        h.started.await();
      }
    }

    void stop() {
      consumer.stopAsync();
      waitForOrderlyStop();
    }

    void waitForOrderlyStop() {
      consumer.stop();

      for (GrantHandler h : grants.values()) {
        h.revoked.await();
        h.finish.open();
      }

      consumer.awaitTerminated();
      consumer.assertTerminated();
    }
  }

  private ConsumerTestDeps setupDeps(TestInfo testInfo) {
    return setupDeps(testInfo, Fixtures.createClock());
  }

  private ConsumerTestDeps setupDeps(TestInfo testInfo, Clock clock) {
    Map<String, GrantHandler> grants = new ConcurrentHashMap<>();

    var instance = Fixtures.newInstance(testInfo.getDisplayName());
    WorkHandler handler =
        (id, shard, ownership) -> {
          var g = grants.get(id);
          g.workHandler.handleWork(id, shard, ownership);
          grants.remove(id, g);
        };
    var consumer = TestConsumer.create(clock, DOMAIN1.service(), instance, handler);

    return new ConsumerTestDeps(instance, grants, consumer);
  }
}
