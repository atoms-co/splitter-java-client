package co.atoms.splitter.consumer;

import static co.atoms.splitter.model.GrantState.ACTIVE;
import static co.atoms.splitter.model.GrantState.ALLOCATED;
import static co.atoms.splitter.model.GrantState.ALLOCATED_LOADED;
import static co.atoms.splitter.model.GrantState.REVOKED;
import static co.atoms.splitter.model.GrantState.REVOKED_UNLOADED;
import static co.atoms.splitter.testing.Asserts.assertCondition;
import static co.atoms.splitter.testing.Asserts.assertElement;
import static co.atoms.splitter.testing.Fixtures.DOMAIN1;
import static co.atoms.splitter.testing.Fixtures.DOMAIN2;
import static co.atoms.splitter.testing.Fixtures.EXP;
import static co.atoms.splitter.testing.Fixtures.INSTANCE;
import static co.atoms.splitter.testing.Fixtures.SHARD_D1_UNIT_0e;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_15;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_5c;
import static co.atoms.splitter.testing.Fixtures.TS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

import co.atoms.splitter.cluster.ClusterId;
import co.atoms.splitter.cluster.ClusterMap;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.proto.JoinMessage;
import co.atoms.splitter.testing.Fixtures;
import co.atoms.splitter.testing.Gate;
import co.atoms.splitter.testing.MutableClock;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(value = 5, threadMode = SEPARATE_THREAD)
@Isolated
public class WorkPoolTest {
  @Test
  public void normalGrantLifecycle() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Activate this grant
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    g.revoked.await();
    assertEquals(g.stateAfterWaitForRevoke.get(), REVOKED);
    deps.assertMetrics(DOMAIN1, LeaseState.REVOKED, 1);

    // Unload this grant
    g.unload.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS)));

    g.unloaded.await();
    assertEquals(g.stateAfterUnload.get(), REVOKED_UNLOADED);

    // Load counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS), grant);

    g.assertFinishedGracefully(REVOKED_UNLOADED);

    assertEquals(g.stateAfterWaitForLoad.get(), REVOKED_UNLOADED);
    deps.assertNoMetrics(DOMAIN1);
    deps.assertMetricsDomains(DOMAIN1); // Kept to reset the metrics

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  // Testing:
  // - grant is activated when waiting for unloading
  // - grant is expired when waiting for revoke
  @Test
  public void grantActivatedWhenWaitingForUnloading() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Activate this grant (skip unloading counterpart)
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    // The grant should be activated
    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ACTIVE);

    // Load this grant
    g.load.open();

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ACTIVE);

    // Grant is activated right away
    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Expire the grant
    deps.clock.advance(Duration.ofSeconds(2));
    deps.scheduler.runScheduled();
    WorkPool.notifyGrantLock(g.ownership());

    g.waitForRevokedFailed.open();

    g.assertFinishedForcefully(ACTIVE);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantRevokedWhenWaitingForUnloading() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedGracefully(REVOKED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantExpiredWhenWaitingForUnloading() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Expire the grant
    deps.clock.advance(Duration.ofSeconds(2));
    deps.scheduler.runScheduled();
    WorkPool.notifyGrantLock(g.ownership());

    // Finish the handler
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedForcefully(ALLOCATED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenWaitingForUnloading() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Release the grant
    g.grantOwnership.get().release();

    // Finish the handler
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedForcefully(ALLOCATED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenAssignedAgainWhileWaitingForUnloading() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Assign again
    deps.assign(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS));

    // Finish the handler
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedForcefully(ALLOCATED);

    g.assertNoFailures("replaced grant gid1");

    deps.assertGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenHandlerExit() {
    var executor = Executors.newSingleThreadExecutor();
    var scheduler = new TestScheduledExecutorService(Executors.newCachedThreadPool());
    LinkedBlockingQueue<JoinMessage> sent = new LinkedBlockingQueue<>();
    AtomicReference<Ownership> ownership = new AtomicReference<>();
    Gate started = new Gate();
    Gate terminated = new Gate();

    WorkHandler handler =
        (id, shard, o) -> {
          ownership.set(o);
          o.onTermination(terminated::open);
          started.open();
        };

    var clock = Fixtures.createClock();
    var pool =
        new WorkPool(
            clock,
            INSTANCE,
            sent::add,
            handler,
            executor,
            scheduler,
            () -> new ClusterMap(ClusterId.DEFAULT, List.of()));
    pool.extend(EXP);

    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    pool.assign(List.of(grant));

    started.await();
    terminated.await();
    assertEquals(ownership.get().getExpiration(), EXP);
    assertEquals(ownership.get().getState(), ALLOCATED);

    assertElement(sent, Messages.createReleased(grant));

    assertCondition(
        () -> {
          scheduler.runScheduled();
          return scheduler.size() == 0;
        });
  }

  // Testing:
  // - grant is assigned as active and bypasses waitForOtherGrantToUnload and waitForActive
  // - grant is released when waiting for revoke
  @Test
  public void assignedActiveGrant() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // The grant should be activated
    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ACTIVE);

    // Load this grant
    g.load.open();

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ACTIVE);

    // Grant is activated right away
    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Release the grant
    g.grantOwnership.get().release();

    g.waitForRevokedFailed.open();

    g.assertFinishedForcefully(ACTIVE);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantRevokedWhenWaitingForActive() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    // Finish the handler
    g.waitForActiveFailed.open();

    g.assertFinishedGracefully(REVOKED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantExpiredWhenWaitingForActive() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Expire the grant
    deps.clock.advance(Duration.ofSeconds(2));
    deps.scheduler.runScheduled();
    WorkPool.notifyGrantLock(g.ownership());

    // Finish the handler
    g.waitForActiveFailed.open();

    g.assertFinishedForcefully(ALLOCATED_LOADED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenAssignedAgainWhileWaitingForActive() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Assign again
    deps.assign(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS));

    // Finish the handler
    g.waitForActiveFailed.open();

    g.assertFinishedForcefully(ALLOCATED_LOADED);

    g.assertNoFailures("replaced grant gid1");

    deps.assertGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenAssignedAgainWhileWaitingForRevoke() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Activate this grant
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Assign again
    deps.assign(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS));

    // Finish the handler
    g.waitForRevokedFailed.open();

    g.assertFinishedForcefully(ACTIVE);

    g.assertNoFailures("replaced grant gid1");

    deps.assertGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhenAssignedAgainWhileWaitingForLoad() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Activate this grant
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    g.revoked.await();
    assertEquals(g.stateAfterWaitForRevoke.get(), REVOKED);

    // Unload this grant
    g.unload.open();

    g.unloaded.await();
    assertEquals(g.stateAfterUnload.get(), REVOKED_UNLOADED);

    // Assign again
    deps.assign(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS));

    // Finish the handler
    g.waitForOtherLoaded.open();

    g.assertFinishedForcefully(REVOKED_UNLOADED);

    g.assertNoFailures("replaced grant gid1");

    deps.assertGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantExpiredWhileWaitingForLoad() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Activate this grant
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    g.revoked.await();
    assertEquals(g.stateAfterWaitForRevoke.get(), REVOKED);

    // Unload this grant
    g.unload.open();

    g.unloaded.await();
    assertEquals(g.stateAfterUnload.get(), REVOKED_UNLOADED);

    // Expire the grant
    deps.clock.advance(Duration.ofSeconds(2));
    deps.scheduler.runScheduled();
    WorkPool.notifyGrantLock(g.ownership());

    // Finish the handler
    g.waitForOtherLoaded.open();

    g.assertFinishedForcefully(REVOKED_UNLOADED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void grantReleasedWhileWaitingForLoad() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    // Unload counterpart
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.otherUnloaded.await();
    assertEquals(g.stateAfterWaitForUnload.get(), ALLOCATED);

    // Load this grant
    g.load.open();
    assertElement(
        deps.sent,
        Messages.createUpdate(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED_LOADED, EXP, TS)));

    g.loaded.await();
    assertEquals(g.stateAfterLoad.get(), ALLOCATED_LOADED);

    // Activate this grant
    deps.pool.promote(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    // Revoke this grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));

    g.revoked.await();
    assertEquals(g.stateAfterWaitForRevoke.get(), REVOKED);

    // Unload this grant
    g.unload.open();

    g.unloaded.await();
    assertEquals(g.stateAfterUnload.get(), REVOKED_UNLOADED);

    // Release the grant
    g.grantOwnership.get().release();

    // Finish the handler
    g.waitForOtherLoaded.open();

    g.assertFinishedForcefully(REVOKED_UNLOADED);

    deps.assertNoGrantsPresent();
    deps.assertNoFailures();
  }

  @Test
  public void extend() {
    var deps = setup();

    // Assign grants
    var grant1 =
        Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP.plus(Duration.ofSeconds(2)), TS);
    var grant2 =
        Grant.create("gid2", SHARD_D2_GLOBAL_15, ALLOCATED, EXP.plus(Duration.ofSeconds(2)), TS);
    var grant3 = Grant.create("gid3", SHARD_D2_GLOBAL_5c, ALLOCATED, EXP, TS);
    deps.assign(grant1, grant2, grant3);
    var g1 = deps.grant(grant1);
    var g2 = deps.grant(grant2);
    var g3 = deps.grant(grant3);
    assertEquals(deps.scheduler.size(), 1);

    // Wait to expire grant3
    deps.clock.advance(Duration.ofSeconds(1));
    deps.scheduler.runScheduled();
    g3.waitForOtherUnloadedFailed.open();
    g3.finished.await();

    assertEquals(deps.scheduler.size(), 1);

    g3.assertFinishedForcefully(ALLOCATED);

    g3.assertNoFailures("grant gid3");

    // Extend the lease and verify that both grants are extended
    var lease = EXP.plusSeconds(5);
    deps.pool.extend(lease);

    assertEquals(g1.ownership().getExpiration(), lease);
    assertEquals(g2.ownership().getExpiration(), lease);

    // New lease caused a new expiration check
    assertEquals(deps.scheduler.size(), 1);

    // Revoke one grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, lease, TS)));

    // Extend the lease again and verify that only the active lease is extended
    var newLease = lease.plusSeconds(5);
    deps.pool.extend(newLease);

    assertEquals(g1.ownership().getExpiration(), lease);
    assertEquals(g2.ownership().getExpiration(), newLease);

    // No new expiration checks, revoked grant is at the old expiration
    assertEquals(deps.scheduler.size(), 1);

    // Disconnect the pool to cause grant to become stale
    deps.pool.disconnected();

    // Extend the lease again and verify that the stale lease is not extended
    deps.pool.extend(newLease.plusSeconds(5));

    assertEquals(g1.ownership().getExpiration(), lease);
    assertEquals(g2.ownership().getExpiration(), newLease);

    // No new expiration checks, revoked grant is at the old expiration
    assertEquals(deps.scheduler.size(), 1);

    deps.assertNoFailures();
  }

  @Test
  public void reconnectWithStaleGrants() {
    var deps = setup();

    // Assign grants
    var grant1 = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    var grant2 = Grant.create("gid2", SHARD_D2_GLOBAL_15, ALLOCATED, EXP, TS);
    deps.assign(grant1, grant2);
    var g1 = deps.grant(grant1);
    var g2 = deps.grant(grant2);
    assertEquals(deps.scheduler.size(), 1);
    assertEquals(deps.pool.getStaleGrants().count(), 0);

    // Disconnect the pool to cause grants to become stale
    deps.pool.disconnected();
    assertEquals(
        deps.pool.getStaleGrants().map(Grant::id).sorted().collect(Collectors.toList()),
        List.of("gid1", "gid2"));
    deps.assertMetrics(DOMAIN1, LeaseState.STALE, 1);
    deps.assertMetrics(DOMAIN2, LeaseState.STALE, 1);
    deps.assertMetricsDomains(DOMAIN1, DOMAIN2);

    // Reconnect the pool and verify that the stale grants are still there
    deps.pool.extend(EXP);
    deps.pool.assign(List.of(grant1, grant2));
    assertFalse(g1.ownership().isTerminated());
    assertFalse(g2.ownership().isTerminated());
    assertEquals(deps.pool.getStaleGrants().count(), 0);
    deps.assertMetrics(DOMAIN1, LeaseState.ACTIVE, 1);
    deps.assertMetrics(DOMAIN2, LeaseState.ACTIVE, 1);
    deps.assertMetricsDomains(DOMAIN1, DOMAIN2);

    // Revoke one grant
    deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, REVOKED, EXP, TS)));
    deps.assertMetrics(DOMAIN1, LeaseState.REVOKED, 1);
    deps.assertMetrics(DOMAIN2, LeaseState.ACTIVE, 1);
    deps.assertMetricsDomains(DOMAIN1, DOMAIN2);

    // Disconnect the pool to cause grant to become stale. Revoked grant won't be affected
    deps.pool.disconnected();
    assertEquals(
        deps.pool.getStaleGrants().map(Grant::id).collect(Collectors.toList()), List.of("gid2"));
    deps.assertMetrics(DOMAIN1, LeaseState.REVOKED, 1);
    deps.assertMetrics(DOMAIN2, LeaseState.STALE, 1);
    deps.assertMetricsDomains(DOMAIN1, DOMAIN2);

    // Re-connect to activate the grant
    var lease = EXP.plusSeconds(5);
    deps.pool.extend(lease);
    deps.pool.assign(List.of(Grant.create("gid2", SHARD_D2_GLOBAL_15, ALLOCATED, lease, TS)));
    assertFalse(g2.ownership().isTerminated());
    deps.assertMetrics(DOMAIN1, LeaseState.REVOKED, 1);
    deps.assertMetrics(DOMAIN2, LeaseState.ACTIVE, 1);
    deps.assertMetricsDomains(DOMAIN1, DOMAIN2);

    assertEquals(g1.ownership().getExpiration(), EXP);
    assertEquals(g2.ownership().getExpiration(), lease);
    assertEquals(deps.pool.getStaleGrants().count(), 0);

    deps.assertNoFailures();
  }

  @Test
  public void disconnectAfterStopWithGrants() {
    var deps = setup();
    var executor = Executors.newSingleThreadExecutor();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    deps.pool.stopAsync();

    var terminated = new Gate();
    executor.submit(
        () -> {
          try {
            deps.pool.awaitTerminated();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          terminated.open();
        });

    // Disconnected after stopping. All known grants are revoked
    deps.pool.disconnected();

    // Finish the handler
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedGracefully(REVOKED);

    deps.assertNoGrantsPresent();

    terminated.await();

    deps.assertNoFailures();
  }

  @Test
  public void disconnectAfterStopWithoutGrants() {
    var deps = setup();
    var executor = Executors.newSingleThreadExecutor();

    deps.pool.stopAsync();

    var terminated = new Gate();
    executor.submit(
        () -> {
          try {
            deps.pool.awaitTerminated();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          terminated.open();
        });

    // Disconnected after stopping.
    deps.pool.disconnected();

    terminated.await();
  }

  @Test
  public void stoppedAfterDisconnectWithGrants() {
    var deps = setup();
    var executor = Executors.newSingleThreadExecutor();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);

    deps.pool.disconnected();

    var terminated = new Gate();
    executor.submit(
        () -> {
          try {
            deps.pool.awaitTerminated();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          terminated.open();
        });

    // Stopped after disconnecting. All known grants are revoked
    deps.pool.stopAsync();

    // Finish the handler
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedGracefully(REVOKED);

    deps.assertNoGrantsPresent();

    terminated.await();

    deps.assertNoFailures();
  }

  @Test
  public void stoppedAfterDisconnectWithoutGrants() {
    var deps = setup();
    var executor = Executors.newSingleThreadExecutor();

    deps.pool.disconnected();

    var terminated = new Gate();
    executor.submit(
        () -> {
          try {
            deps.pool.awaitTerminated();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          terminated.open();
        });

    // Stopped after disconnecting.
    deps.pool.stopAsync();

    terminated.await();

    deps.assertNoFailures();
  }

  @Test
  void promotedUnknownGrantIsReleased() {
    var deps = setup();

    // Unknown promoted grant is released
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.pool.promote(List.of(grant));
    assertElement(deps.sent, Messages.createReleased(grant));

    deps.assertNoFailures();
  }

  @Test
  void promotedStaleGrantIsReleased() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.started.await();

    // Re-connect to make the grant stale
    deps.pool.disconnected();
    deps.pool.extend(EXP);

    // Promote stale grant
    deps.pool.promote(List.of(grant));
    assertElement(deps.sent, Messages.createReleased(grant));

    // Grant is terminated
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedForcefully(ALLOCATED);

    deps.assertNoFailures();
  }

  @Test
  void promotedRevokedGrantIsReleased() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.started.await();

    deps.pool.revoke(List.of(grant));

    // Promote revoked grant
    deps.pool.promote(List.of(grant));
    assertElement(deps.sent, Messages.createReleased(grant));

    // Grant is terminated
    g.waitForOtherUnloadedFailed.open();

    g.assertFinishedForcefully(REVOKED);

    deps.assertNoFailures();
  }

  @Test
  void promotedActiveGrantIsReleased() {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.otherUnloaded.await();
    g.load.open();

    // Promote active grant
    deps.pool.promote(List.of(grant));
    assertElement(deps.sent, Messages.createReleased(grant));

    // Grant is terminated
    g.waitForRevokedFailed.open();

    g.assertFinishedForcefully(ACTIVE);

    deps.assertNoFailures();
  }

  @Test
  void revokedUnknownGrantIsIgnored() {
    var deps = setup();

    // Unknown revoked grant is ignored
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.pool.revoke(List.of(grant));
    assertEquals(deps.sent.size(), 0);

    deps.assertNoFailures();
  }

  @Test
  void updatedUnknownGrantIsIgnored() {
    var deps = setup();

    // Unknown updated grant is ignored
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.pool.update(grant, grant);
    assertEquals(deps.sent.size(), 0);

    deps.assertNoFailures();
  }

  @Test
  void getCluster() {
    ClusterMap cluster = new ClusterMap(ClusterId.DEFAULT, List.of());
    Gate start = new Gate();
    Gate finish = new Gate();
    AtomicReference<Ownership> ownership = new AtomicReference<>();

    var pool =
        new WorkPool(
            Fixtures.createClock(),
            INSTANCE,
            m -> {},
            (id, shard, o) -> {
              ownership.set(o);
              start.open();
              finish.await();
            },
            Executors.newSingleThreadExecutor(),
            new TestScheduledExecutorService(MoreExecutors.newDirectExecutorService()),
            () -> cluster);
    pool.extend(EXP);

    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    pool.assign(List.of(grant));

    start.await();
    assertFalse(ownership.get().isTerminated());
    assertEquals(ownership.get().getCluster(), cluster);
    finish.open();
  }

  @ParameterizedTest
  @EnumSource(
      value = GrantState.class,
      names = {"ALLOCATED"},
      mode = EnumSource.Mode.EXCLUDE)
  void loadWithInvalidStateIsIgnored(GrantState state) {
    LinkedBlockingQueue<JoinMessage> sent = new LinkedBlockingQueue<>();
    Gate loaded = new Gate();
    Gate finish = new Gate();

    var pool =
        new WorkPool(
            Fixtures.createClock(),
            INSTANCE,
            sent::add,
            (id, shard, o) -> {
              o.load();
              loaded.open();
              finish.await();
            },
            Executors.newSingleThreadExecutor(),
            new TestScheduledExecutorService(MoreExecutors.newDirectExecutorService()),
            () -> new ClusterMap(ClusterId.DEFAULT, List.of()));
    pool.extend(EXP);

    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, state, EXP, TS);
    pool.assign(List.of(grant));

    loaded.await();
    assertEquals(sent.size(), 0);

    finish.open();
  }

  @ParameterizedTest
  @EnumSource(
      value = GrantState.class,
      names = {"REVOKED"},
      mode = EnumSource.Mode.EXCLUDE)
  void unloadWithInvalidStateIsIgnored(GrantState state) {
    LinkedBlockingQueue<JoinMessage> sent = new LinkedBlockingQueue<>();
    Gate unloaded = new Gate();
    Gate finish = new Gate();

    var pool =
        new WorkPool(
            Fixtures.createClock(),
            INSTANCE,
            sent::add,
            (id, shard, o) -> {
              o.unload();
              unloaded.open();
              finish.await();
            },
            Executors.newSingleThreadExecutor(),
            new TestScheduledExecutorService(MoreExecutors.newDirectExecutorService()),
            () -> new ClusterMap(ClusterId.DEFAULT, List.of()));
    pool.extend(EXP);

    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, state, EXP, TS);
    pool.assign(List.of(grant));

    unloaded.await();
    assertEquals(sent.size(), 0);

    finish.open();
  }

  @ParameterizedTest
  @EnumSource(
      value = GrantState.class,
      names = {"ALLOCATED", "ALLOCATED_LOADED"})
  void activatedStaleGrantWithNewState(GrantState state) {
    var deps = setup();

    // Assign a grant
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, state, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.started.await();

    g.load.open();

    // Re-connect to make the grant stale
    deps.pool.disconnected();
    deps.pool.extend(EXP);

    // Assign the same grant, but with active state
    deps.pool.assign(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ACTIVE, EXP, TS)));

    g.active.await();
    assertEquals(g.stateAfterWaitForActive.get(), ACTIVE);

    g.ownership().release();

    // Grant is terminated
    g.waitForRevokedFailed.open();

    g.assertFinishedForcefully(ACTIVE);

    deps.assertNoFailures();
  }

  static GrantState[][] invalidStaleGrantStates() {
    return new GrantState[][] {
      {ALLOCATED, REVOKED},
      {ALLOCATED, REVOKED_UNLOADED},
      {ALLOCATED_LOADED, REVOKED},
      {ALLOCATED_LOADED, REVOKED_UNLOADED},
      {ACTIVE, REVOKED},
      {ACTIVE, REVOKED_UNLOADED},
      {REVOKED, ALLOCATED},
      {REVOKED, ALLOCATED_LOADED},
      {REVOKED, ACTIVE},
      {REVOKED, REVOKED_UNLOADED},
      {REVOKED_UNLOADED, ALLOCATED},
      {REVOKED_UNLOADED, ALLOCATED_LOADED},
      {REVOKED_UNLOADED, ACTIVE},
      {REVOKED_UNLOADED, REVOKED},
    };
  }

  @ParameterizedTest
  @MethodSource("invalidStaleGrantStates")
  void recreatedStaleGrantsWithInvalidStates(GrantState oldState, GrantState newState) {
    var deps = setup();

    // Assign a grant
    var state = oldState;
    if (oldState.isInRevokedState()) {
      state = ACTIVE;
    }
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, state, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.started.await();

    if (oldState != ALLOCATED) {
      g.load.open();
    }

    if (!oldState.isInAllocatedState()) {
      g.active.await();
    }

    if (oldState.isInRevokedState()) {
      deps.pool.revoke(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, oldState, EXP, TS)));
      g.unload.open();
      g.unloaded.await();
    }

    // Re-connect to make the grant stale
    deps.pool.disconnected();
    deps.pool.extend(EXP);

    // Assign the same grant, but with new state
    deps.assign(Grant.create("gid1", SHARD_D1_UNIT_0e, newState, EXP, TS));

    g.waitForOtherUnloadedFailed.open();
    g.waitForActiveFailed.open();
    g.waitForRevokedFailed.open();
    g.waitForOtherLoaded.open();

    g.assertFinishedForcefully(oldState.isInRevokedState() ? REVOKED_UNLOADED : oldState);

    // Grant is activated.
    assertEquals(deps.pool.getStaleGrants().count(), 0);

    deps.assertNoFailures();
  }

  @Test
  void reassignedStaleAllocatedGrantToLoadedGrant() {
    var deps = setup();

    // Assign an allocated grant, which will be loaded
    var grant = Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS);
    deps.assign(grant);
    var g = deps.grant(grant);
    g.started.await();
    g.load.open();

    // Load the grant
    deps.pool.update(Grant.create("gid2", SHARD_D1_UNIT_0e, REVOKED_UNLOADED, EXP, TS), grant);

    g.loaded.await();

    // Re-connect to make the grant stale
    deps.pool.disconnected();
    deps.pool.extend(EXP);

    // Assign the same grant, but with old (allocated) state
    deps.pool.assign(List.of(Grant.create("gid1", SHARD_D1_UNIT_0e, ALLOCATED, EXP, TS)));

    // Old state is kept.
    assertEquals(g.ownership().getState(), ALLOCATED_LOADED);

    // Grant is activated.
    assertEquals(deps.pool.getStaleGrants().count(), 0);

    deps.assertNoFailures();
  }

  private static class GrantHandler implements WorkHandler {
    final AtomicReference<Ownership> grantOwnership = new AtomicReference<>();
    final Gate started = new Gate();
    final Gate waitForOtherUnloadedFailed = new Gate();
    final Gate otherUnloaded = new Gate();
    final Gate load = new Gate();
    final Gate loaded = new Gate();
    final Gate active = new Gate();
    final Gate waitForActiveFailed = new Gate();
    final Gate revoked = new Gate();
    final Gate waitForRevokedFailed = new Gate();
    final Gate unload = new Gate();
    final Gate unloaded = new Gate();
    final Gate waitForOtherLoaded = new Gate();
    final Gate finished = new Gate();
    final Gate terminated = new Gate();
    final AtomicReference<GrantState> stateAfterWaitForUnload = new AtomicReference<>();
    final AtomicReference<GrantState> stateAfterLoad = new AtomicReference<>();
    final AtomicReference<GrantState> stateAfterWaitForActive = new AtomicReference<>();
    final AtomicReference<GrantState> stateAfterWaitForRevoke = new AtomicReference<>();
    final AtomicReference<GrantState> stateAfterWaitForLoad = new AtomicReference<>();
    final AtomicReference<GrantState> stateAfterUnload = new AtomicReference<>();
    final AtomicReference<GrantState> finalState = new AtomicReference<>();
    final AtomicBoolean released = new AtomicBoolean();

    Ownership ownership() {
      return grantOwnership.get();
    }

    @Override
    public void handleWork(String id, Shard shard, Ownership ownership) {
      grantOwnership.set(ownership);
      ownership.onTermination(terminated::open);
      try {
        started.open();

        if (!ownership.waitForOtherGrantToUnload()) {
          waitForOtherUnloadedFailed.await();
          return;
        }
        stateAfterWaitForUnload.set(ownership.getState());
        otherUnloaded.open();

        load.await();
        ownership.load();
        stateAfterLoad.set(ownership.getState());
        loaded.open();

        if (!ownership.waitForActive()) {
          waitForActiveFailed.await();
          return;
        }
        stateAfterWaitForActive.set(ownership.getState());
        active.open();

        if (!ownership.waitForRevoked()) {
          waitForRevokedFailed.await();
          return;
        }
        stateAfterWaitForRevoke.set(ownership.getState());
        revoked.open();

        unload.await();
        ownership.unload();
        stateAfterUnload.set(ownership.getState());
        unloaded.open();

        if (!ownership.waitForOtherGrantToLoad()) {
          waitForOtherLoaded.await();
          return;
        }
        stateAfterWaitForLoad.set(ownership.getState());
      } finally {
        finalState.set(ownership.getState());
        if (ownership.isTerminated()) {
          released.set(true);
        }
        finished.open();
      }
    }

    /**
     * Checks that there are no failures in the grant handler. These failures may not be caught by
     * the test itself due to asynchronous nature of the handler.
     */
    void assertNoFailures(String message) {
      assertFalse(waitForOtherUnloadedFailed.failed(), message + ": waitForOtherUnloadedFailed");
      assertFalse(load.failed(), message + ": load");
      assertFalse(waitForActiveFailed.failed(), message + ": waitForActiveFailed");
      assertFalse(waitForRevokedFailed.failed(), message + ": waitForRevokedFailed");
      assertFalse(unload.failed(), message + ": unload");
      assertFalse(waitForOtherLoaded.failed(), message + ": waitForOtherLoaded");
    }

    /**
     * Handler is terminated gracefully when it exited without the pool initiating the release of
     * the grant.
     */
    void assertFinishedGracefully(GrantState finalState) {
      finished.await();
      terminated.await();
      assertEquals(this.finalState.get(), finalState);
      assertTrue(ownership().isTerminated());

      // Terminated gracefully
      assertFalse(released.get());
    }

    /**
     * Handler is terminated forcefully when it exited after the pool initiated the release of the
     * grant.
     */
    void assertFinishedForcefully(GrantState finalState) {
      finished.await();
      terminated.await();
      assertEquals(this.finalState.get(), finalState);
      assertEquals(ownership().getState(), finalState);
      assertTrue(ownership().isTerminated());

      // Terminated gracefully
      assertTrue(released.get());
    }
  }

  private static class WorkPoolTestDeps {
    MutableClock clock;
    WorkPool pool;
    TestScheduledExecutorService scheduler;
    LinkedBlockingQueue<JoinMessage> sent;
    ConcurrentHashMap<String, GrantHandler> grants = new ConcurrentHashMap<>();

    /** Assigns grants to the pool and waits for the handlers to start. */
    void assign(Grant... grants) {
      for (var g : grants) {
        var h = new GrantHandler();
        this.grants.put(g.id(), h);
      }
      pool.assign(Arrays.asList(grants));
      var domains = new HashMap<QualifiedDomainName, Integer>();
      for (var g : grants) {
        var h = this.grants.get(g.id());
        h.started.await();
        assertEquals(h.ownership().getExpiration(), g.lease());
        assertEquals(h.ownership().getState(), g.state());
        assertFalse(h.ownership().isTerminated());
        domains.merge(g.shard().domain(), 1, Integer::sum);
      }
      for (var d : domains.entrySet()) {
        assertMetrics(d.getKey(), LeaseState.ACTIVE, d.getValue());
      }
      assertMetricsDomains(domains.keySet().toArray(new QualifiedDomainName[0]));
    }

    GrantHandler grant(Grant grant) {
      return grants.get(grant.id());
    }

    /**
     * Checks that grants are not present by verifying that there is no task to check expiration of
     * leases.
     */
    void assertNoGrantsPresent() {
      assertCondition(
          () -> {
            scheduler.runScheduled();
            return scheduler.size() == 0;
          });
    }

    /**
     * Checks that grants are present by verifying that there is a task to check expiration of
     * leases.
     */
    void assertGrantsPresent() {
      assertCondition(
          () -> {
            scheduler.runScheduled();
            return scheduler.size() == 1;
          });
    }

    void assertNoFailures() {
      for (var g : grants.entrySet()) {
        g.getValue().assertNoFailures(String.format("grant %s has failures", g.getKey()));
      }
    }

    void assertMetrics(QualifiedDomainName domain, LeaseState state, long count) {
      var metrics = pool.collectMetrics();
      assertEquals(metrics.get(domain).get(state), count);
    }

    void assertNoMetrics(QualifiedDomainName domain, LeaseState state) {
      var metrics = pool.collectMetrics();
      assertEquals(metrics.get(domain).get(state), 0);
    }

    void assertNoMetrics(QualifiedDomainName domain) {
      var metrics = pool.collectMetrics();
      assertFalse(
          metrics.containsKey(domain) && metrics.get(domain).values().stream().anyMatch(c -> c > 0),
          "Expected no metrics, but got: " + metrics.get(domain));
    }

    void assertMetricsDomains(QualifiedDomainName... domains) {
      pool.collectMetrics();
      var given =
          pool.getMetricDomains().keySet().stream()
              .sorted(Comparator.comparing(QualifiedDomainName::name))
              .collect(Collectors.toList());
      var expected =
          Arrays.stream(domains)
              .sorted(Comparator.comparing(QualifiedDomainName::name))
              .collect(Collectors.toList());
      assertEquals(given, expected);
    }
  }

  private static WorkPoolTestDeps setup() {
    var deps = new WorkPoolTestDeps();

    var executor = Executors.newCachedThreadPool();
    deps.scheduler = new TestScheduledExecutorService(MoreExecutors.newDirectExecutorService());
    deps.sent = new LinkedBlockingQueue<>();

    WorkHandler handler =
        (id, shard, ownership) -> {
          var g = deps.grants.get(id);
          g.handleWork(id, shard, ownership);
          deps.grants.remove(id, g);
        };

    deps.clock = Fixtures.createClock();
    deps.pool =
        new WorkPool(
            deps.clock,
            INSTANCE,
            deps.sent::add,
            handler,
            executor,
            deps.scheduler,
            () -> new ClusterMap(ClusterId.DEFAULT, List.of()));
    deps.assertMetricsDomains();
    deps.pool.extend(EXP);
    return deps;
  }

  /**
   * A {@link ScheduledExecutorService} that accumulates scheduled tasks and runs them on demand.
   */
  static class TestScheduledExecutorService extends AbstractExecutorService
      implements ScheduledExecutorService {
    private final ExecutorService executor;
    private final List<Runnable> scheduled = new ArrayList<>();

    TestScheduledExecutorService(ExecutorService executor) {
      this.executor = executor;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      scheduled.add(command);
      return new Job<>(command, 0, Duration.ofNanos(unit.toNanos(delay)));
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
      executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return executor.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
      executor.execute(command);
    }

    /**
     * Runs all tasks scheduled up to this moment. If new tasks are scheduled as a result of
     * executing the scheduled tasks, they are not executed.
     */
    public void runScheduled() {
      var scheduled = new ArrayList<>(this.scheduled);
      this.scheduled.clear();
      for (var job : scheduled) {
        executor.execute(job);
      }
    }

    public int size() {
      return scheduled.size();
    }
  }

  static class Job<V> extends FutureTask<V> implements ScheduledFuture<V> {
    private final Duration delay;

    Job(Runnable callable, V result, Duration delay) {
      super(callable, result);
      this.delay = delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
