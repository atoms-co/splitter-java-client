package co.atoms.splitter.consumer;

import co.atoms.splitter.cluster.Cluster;
import co.atoms.splitter.internal.Metrics;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.proto.JoinMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkPool manages the work assigned to a consumer. It handles creation of handlers on assignments,
 * updates in grant states and termination of handlers.
 *
 * <p>The pool is controlled by the consumer by invoking assignment lifecycle methods (assign,
 * revoke, update, promote) originated by coordinator. The handlers control their state and the pool
 * propagates these updates back to the consumer.
 *
 * <p>Handlers (including possible termination listeners) are executed using the provided executor
 * service.
 */
class WorkPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkPool.class);
  private static final String CLIENT_VERSION = co.atoms.splitter.consumer.Consumer.CLIENT_VERSION;
  // Defines how often current locally owned grant states are logged.
  private static final long GRANT_LOG_INTERVAL_MILLIS = Duration.ofMinutes(10).toMillis();
  private static final long GRANT_LOG_JITTER_MILLIS = Duration.ofSeconds(1).toMillis();

  private final Clock clock;
  private final QualifiedServiceName service;
  private final Instance consumer;
  private final Consumer<JoinMessage> toServer;
  private final WorkHandler handler;
  private final ExecutorService handlerPool;
  private final Executor grantLogExecutor;
  private final ScheduledExecutorService scheduler;
  private final Supplier<Cluster> clusterSupplier;
  private final AtomicBoolean grantLogPending = new AtomicBoolean();
  private volatile @Nullable ScheduledFuture<?> grantLogTask;

  private @Nullable Instant lease;
  private final Map<String, GrantInfo> grants = new HashMap<>();
  private final ReentrantLock lock = new ReentrantLock();
  private boolean connected = false;
  private boolean stopping = false;
  private final Condition stopped = lock.newCondition();
  private @Nullable Instant scheduledExpiration;

  // Contains the domains for which metrics have been collected. Used to reset domains that are no
  // longer present.
  private final Map<QualifiedDomainName, Boolean> metricDomains = new ConcurrentHashMap<>();

  @VisibleForTesting
  WorkPool(
      Clock clock,
      Instance consumer,
      Consumer<JoinMessage> toServer,
      WorkHandler handler,
      ExecutorService handlerPool,
      ScheduledExecutorService scheduler,
      Supplier<Cluster> clusterSupplier) {
    this(
        clock,
        QualifiedServiceName.create("test", "test"),
        consumer,
        toServer,
        handler,
        handlerPool,
        handlerPool,
        scheduler,
        clusterSupplier);
  }

  public WorkPool(
      Clock clock,
      QualifiedServiceName service,
      Instance consumer,
      Consumer<JoinMessage> toServer,
      WorkHandler handler,
      ExecutorService handlerPool,
      Executor grantLogExecutor,
      ScheduledExecutorService scheduler,
      Supplier<Cluster> clusterSupplier) {
    this.clock = clock;
    this.service = service;
    this.consumer = consumer;
    this.toServer = toServer;
    this.handler = handler;
    this.handlerPool = handlerPool;
    this.grantLogExecutor = grantLogExecutor;
    this.scheduler = scheduler;
    this.clusterSupplier = clusterSupplier;
    this.lease = null;
  }

  public void init() {
    scheduler.scheduleWithFixedDelay(this::emitMetrics, 15, 15, TimeUnit.SECONDS);
    var grantLogInterval =
        GRANT_LOG_INTERVAL_MILLIS + ThreadLocalRandom.current().nextLong(GRANT_LOG_JITTER_MILLIS);
    grantLogTask =
        scheduler.scheduleWithFixedDelay(
            this::logGrantsSafely, grantLogInterval, grantLogInterval, TimeUnit.MILLISECONDS);
  }

  public Stream<Grant> getStaleGrants() {
    try {
      lock.lock();
      return grants.values().stream()
          .filter(info -> info.state == LeaseState.STALE)
          .map(GrantInfo::getGrant);
    } finally {
      lock.unlock();
    }
  }

  public void extend(Instant lease) {
    try {
      lock.lock();
      this.lease = lease;
      grants.forEach(
          (id, info) -> {
            if (info.state == LeaseState.ACTIVE) {
              info.ownership.setExpiration(lease);
            }
          });
      connected = true;
      scheduleLeaseExpirationCheck();
      logStatus();
    } finally {
      lock.unlock();
    }
  }

  /** Signals that the session has been disconnected from the coordinator. */
  public void disconnected() {
    try {
      lock.lock();
      for (var info : grants.values()) {
        if (info.state != LeaseState.REVOKED) {
          info.state = LeaseState.STALE;
          LOGGER.info("WorkPool {} marked grant {} as stale", consumer, info);
        }
      }
      connected = false;
      if (stopping) {
        // Disconnected after shutting down. Revoke all grants locally. The workpool won't be
        // connected to the server.
        for (var info : grants.values()) {
          info.ownership.revokeIfNotRevoked();
          LOGGER.info(
              "WorkPool {} revoked grant {} while disconnecting after stopping", consumer, info);
        }
      }
      notifyIfStopped();
    } finally {
      lock.unlock();
    }
  }

  public void assign(List<Grant> assigned) {
    try {
      lock.lock();
      var lease = this.lease;
      if (lease == null) {
        LOGGER.error("Internal: assigning grants before lease is extended. Releasing");
        for (var g : assigned) {
          send(Messages.createReleased(g));
        }
        return;
      }

      for (Grant g : assigned) {
        var old = grants.get(g.id());
        if (old != null) {
          if (old.state == LeaseState.STALE) {
            if (updateStaleGrant(old, g)) {
              old.state = LeaseState.ACTIVE;
              old.ownership.setExpiration(lease);
              LOGGER.info("WorkPool {} activated stale grant {}", consumer, old);
              continue;
            }
            LOGGER.error(
                "Internal: unexpected assignment of grant in invalid state. "
                    + "Re-creating. Old grant: {}. New grant: {}",
                old,
                g);
          } else {
            LOGGER.error(
                "Internal: unexpected assignment of non-stale grant. "
                    + "Re-creating. Old: {}. New: {}",
                old,
                g);
          }
          old.ownership.release();
        }

        var info =
            new GrantInfo(
                clock,
                g,
                LeaseState.ACTIVE,
                this.handler,
                handlerPool,
                this::updateGrant,
                clusterSupplier);
        grants.put(g.id(), info);
        var grant = info.getGrant();
        logGrantEvent(
            "Consumer grant assigned", grant, GrantLogging.EventType.ASSIGN, null, grant.state());
        handlerPool.submit(() -> runHandler(info));
      }
      scheduleLeaseExpirationCheck();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Update a stale grant with the new assigned grant. Stale grants are sent to the server during
   * registering for re-assignment.
   *
   * <p>The stale grants don't include revoked grants. If a stale grant was released by the client,
   * it is removed from the grants, and it won't appear in stale grants. As a result, stale grants
   * contain either allocated or active grants.
   *
   * <p>The new grant can be in the same state as the stale grant, or it can be in an advanced
   * state. For example, an allocated grant became stale and while re-connecting to the server, it
   * was activated. In such cases, the owner of the grant must be notified that the grant advanced
   * to a new state.
   *
   * <p>Note that the server is considered the source of truth for all cases except loaded and
   * unloaded, which are set by the client. In case when local state conflicts with the server the
   * update fails.
   *
   * <p>Returns true if update was successful
   */
  private boolean updateStaleGrant(GrantInfo old, Grant newGrant) {
    var oldState = old.ownership.getState();
    var newState = newGrant.state();

    if (oldState == GrantState.UNKNOWN || newState == GrantState.UNKNOWN) {
      return false;
    }

    if (oldState == newState) {
      return true;
    }

    switch (newState) {
      case ALLOCATED:
        // Grant advanced locally, but server grant is still allocated. The update will eventually
        // reach the server.
        return oldState == GrantState.ALLOCATED_LOADED;
      case ALLOCATED_LOADED:
        // New grant can't be loaded without old being loaded. Invalid update.
        return false;
      case ACTIVE:
        if (oldState.isInAllocatedState()) {
          // Local grant is allocated. Notify handler about the activation.
          old.ownership.activate(newGrant.lease());
          logGrantEvent(
              "Consumer grant promoted",
              old.getGrant(),
              GrantLogging.EventType.PROMOTE,
              oldState,
              old.getGrant().state());
          return true;
        }
        return false;
      case REVOKED:
      case REVOKED_UNLOADED:
        // Grants are revoked explicitly using revoke message, not assign.
        return false;
    }
    return false;
  }

  private void runHandler(GrantInfo info) {
    try {
      info.handler.handleWork(info.id, info.shard, info.ownership);
    } catch (Throwable t) {
      LOGGER.error("Work handler failed for grant {}. Releasing grant", info, t);
    } finally {
      removeGrant(info);
      scheduleLeaseExpirationCheck();
    }
  }

  private void removeGrant(GrantInfo info) {
    try {
      lock.lock();

      info.ownership.release();

      var current = grants.get(info.id);
      if (current == null) {
        LOGGER.warn("Internal: removing unregistered grant {}. Ignoring", info);
        return;
      }
      if (info != current) {
        LOGGER.error("Internal: removing replaced grant {} (current: {}). Ignoring", info, current);
        return;
      }

      if (info.state != LeaseState.STALE && !info.ownership.isExpired()) {
        send(Messages.createReleased(info.getGrant()));
      }
      var removed = current.getGrant();
      grants.remove(info.id);

      notifyIfStopped();
      Metrics.recordDeletedGrantDuration(
          removed.shard().domain(),
          Duration.between(removed.assigned(), Instant.now()).toSeconds());
      logGrantEvent(
          "Consumer grant removed", removed, GrantLogging.EventType.REMOVE, removed.state(), null);
      LOGGER.info("WorkPool {} removed grant {}", consumer, info);
    } finally {
      lock.unlock();
    }
  }

  private void updateGrant(Update update) {
    Grant grant;
    try {
      lock.lock();
      var info = grants.get(update.grantId);
      if (info == null) {
        LOGGER.error("Internal: updating stale grant {}. Ignoring", update.grantId);
        return;
      }
      grant = info.getGrant();
    } finally {
      lock.unlock();
    }

    if (update.state == GrantState.ALLOCATED_LOADED) {
      logGrantEvent(
          "Consumer grant loaded",
          grant,
          GrantLogging.EventType.UPDATE,
          update.fromState,
          update.state);
      send(Messages.createUpdate(grant));
      LOGGER.info("WorkPool {} sent grant update for {}", consumer, grant);
    }

    if (update.state == GrantState.REVOKED_UNLOADED) {
      logGrantEvent(
          "Consumer grant unloaded",
          grant,
          GrantLogging.EventType.UPDATE,
          update.fromState,
          update.state);
      send(Messages.createUpdate(grant));
      LOGGER.info("WorkPool {} sent grant update for {}", consumer, grant);
    }
  }

  public void promote(List<Grant> promoted) {
    try {
      lock.lock();
      for (var g : promoted) {
        var info = grants.get(g.id());
        if (info == null) {
          LOGGER.error("Internal: unexpected promotion of unowned grant {}. Releasing grant", g);
          send(Messages.createReleased(g));
          continue;
        }
        if (info.state != LeaseState.ACTIVE) {
          LOGGER.error(
              "Internal: unexpected promotion of inactive grant {}. "
                  + "Releasing and closing grant",
              g);
          send(Messages.createReleased(g));
          info.ownership.release();
          continue;
        }
        if (!info.ownership.getState().isInAllocatedState()) {
          LOGGER.error(
              "Internal: unexpected promotion of non-allocated grant {}. "
                  + "Releasing and closing grant",
              g);
          send(Messages.createReleased(g));
          info.ownership.release();
          continue;
        }
        var fromState = info.ownership.getState();
        info.ownership.activate(g.lease());
        logGrantEvent(
            "Consumer grant promoted",
            info.getGrant(),
            GrantLogging.EventType.PROMOTE,
            fromState,
            info.getGrant().state());
        LOGGER.info("Promoted grant to active: {}", info);
      }
      scheduleLeaseExpirationCheck();
    } finally {
      lock.unlock();
    }
  }

  public void revoke(List<Grant> revoked) {
    try {
      lock.lock();
      for (var g : revoked) {
        var info = grants.get(g.id());
        if (info == null) {
          LOGGER.error("Revoking stale grant: {}. Ignoring", g);
          continue;
        }
        LOGGER.info("Revoking grant {}, ttl={}", g, lease);
        var fromState = info.ownership.getState();
        info.state = LeaseState.REVOKED;
        info.ownership.revoke(g.lease());
        logGrantEvent(
            "Consumer grant revoked",
            info.getGrant(),
            GrantLogging.EventType.REVOKE,
            fromState,
            info.getGrant().state());
      }
      scheduleLeaseExpirationCheck();
    } finally {
      lock.unlock();
    }
  }

  public void update(Grant update, Grant target) {
    LOGGER.info("Received grant update {}, for target {}", update, target);

    GrantInfo info;
    try {
      lock.lock();
      info = grants.get(target.id());
      if (info == null) {
        LOGGER.warn("Updating stale grant: {}. Ignoring", target);
        return;
      }
    } finally {
      lock.unlock();
    }

    switch (update.state()) {
      case ALLOCATED_LOADED:
        info.ownership.otherGrantLoaded();
        break;
      case REVOKED_UNLOADED:
        info.ownership.otherGrantUnloaded();
        break;
      default:
        LOGGER.warn("Received grant update with unexpected state: {}", update);
    }
  }

  private void scheduleLeaseExpirationCheck() {
    try {
      lock.lock();
      Instant minExpiration = scheduledExpiration;
      for (var info : grants.values()) {
        if (minExpiration == null || info.ownership.getExpiration().isBefore(minExpiration)) {
          minExpiration = info.ownership.getExpiration();
        }
      }
      if (minExpiration != null && !minExpiration.equals(scheduledExpiration)) {
        var duration = Duration.between(Instant.now(clock), minExpiration);
        scheduler.schedule(this::checkLeaseExpiration, duration.toMillis(), TimeUnit.MILLISECONDS);
        scheduledExpiration = minExpiration;
      }
    } finally {
      lock.unlock();
    }
  }

  private void checkLeaseExpiration() {
    try {
      lock.lock();
      if (isStopped()) {
        return;
      }

      var now = Instant.now(clock);
      var infos = new ArrayList<>(grants.values());
      for (var info : infos) {
        if (!info.ownership.getExpiration().isAfter(now)) {
          removeGrant(info);
        }
      }
      scheduledExpiration = null;
      if (!grants.isEmpty()) {
        scheduleLeaseExpirationCheck();
      }
      logStatus();
    } finally {
      lock.unlock();
    }
  }

  private void send(JoinMessage msg) {
    toServer.accept(msg);
  }

  public void stopAsync() {
    try {
      lock.lock();
      stopping = true;
      if (!connected) {
        // Revoke all grants locally. The workpool won't be connected to the server.
        for (var info : grants.values()) {
          info.ownership.revokeIfNotRevoked();
        }
      }
      notifyIfStopped();
    } finally {
      lock.unlock();
    }
  }

  private boolean isStopped() {
    return stopping && !connected && grants.isEmpty();
  }

  private void notifyIfStopped() {
    if (!isStopped()) {
      return;
    }

    var logTask = grantLogTask;
    if (logTask != null) {
      logTask.cancel(false);
    }
    stopped.signalAll();
  }

  public void awaitTerminated() throws InterruptedException {
    try {
      lock.lock();
      while (!isStopped()) {
        stopped.await();
      }
    } finally {
      lock.unlock();
    }
  }

  private void emitMetrics() {
    var metrics = collectMetrics();

    // Record metrics
    for (var e : metrics.entrySet()) {
      var domain = e.getKey();
      var counts = e.getValue();
      for (var entry : counts.entrySet()) {
        var state = entry.getKey();
        var count = entry.getValue();
        Metrics.recordShards(domain, state.name(), count);
      }
    }
  }

  private void logStatus() {
    LOGGER.info(
        "WorkPool {}: lease={} #grants={}, connected={}, stopping={}, expiration={}",
        consumer,
        lease,
        grants.size(),
        connected,
        stopping,
        scheduledExpiration);
  }

  private void logGrantEvent(
      String message,
      Grant grant,
      GrantLogging.EventType eventType,
      @Nullable GrantState fromState,
      @Nullable GrantState toState) {
    var event =
        new GrantLogging.GrantEvent(
            eventType,
            Instant.now(clock),
            service,
            consumer,
            grant,
            fromState,
            toState,
            CLIENT_VERSION);
    GrantLogging.logGrantEvent(LOGGER, message, event);
  }

  private void logGrantsSafely() {
    GrantLogging.CurrentGrants currentGrants;
    Instant at;
    try {
      at = Instant.now(clock);
      currentGrants = collectCurrentGrants();
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to collect consumer grants", e);
      return;
    }

    if (!grantLogPending.compareAndSet(false, true)) {
      LOGGER.warn("Skipping consumer grant log: worker busy");
      return;
    }

    try {
      grantLogExecutor.execute(
          () -> {
            try {
              GrantLogging.logGrants(LOGGER, "Consumer grant states", currentGrants, at);
            } catch (RuntimeException e) {
              LOGGER.warn("Failed to log consumer grant states", e);
            } finally {
              grantLogPending.set(false);
            }
          });
    } catch (RejectedExecutionException e) {
      grantLogPending.set(false);
      LOGGER.warn("Failed to schedule consumer grant log", e);
    }
  }

  @VisibleForTesting
  GrantLogging.CurrentGrants collectCurrentGrants() {
    try {
      lock.lock();
      var currentGrants = new ArrayList<Grant>(grants.size());
      for (var info : grants.values()) {
        currentGrants.add(info.getGrant());
      }

      return new GrantLogging.CurrentGrants(
          service, consumer, connected, stopping, CLIENT_VERSION, List.copyOf(currentGrants));
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  Map<QualifiedDomainName, Map<LeaseState, Long>> collectMetrics() {
    var metrics = new HashMap<QualifiedDomainName, Map<LeaseState, Long>>();
    // Set all states of previously known domains to zero to reset the metrics
    for (var domain : metricDomains.keySet()) {
      metrics.computeIfAbsent(
          domain,
          d -> {
            var m = new HashMap<LeaseState, Long>();
            // Initialize all states to 0 to reset the metrics.
            for (var state : LeaseState.values()) {
              m.put(state, 0L);
            }
            return m;
          });
    }

    // Collect the metrics from current grants
    try {
      lock.lock();
      for (var g : grants.values()) {
        var domain = g.shard.domain();
        metrics.computeIfAbsent(domain, d -> new HashMap<>()).merge(g.state, 1L, Long::sum);
      }
    } finally {
      lock.unlock();
    }

    // Update known recorded domains
    metricDomains.clear();
    for (var domain : metrics.keySet()) {
      metricDomains.put(domain, true);
    }

    return metrics;
  }

  @VisibleForTesting
  Map<QualifiedDomainName, Boolean> getMetricDomains() {
    return metricDomains;
  }

  private static class GrantInfo {
    final String id;
    final Shard shard;
    volatile LeaseState state;
    final Instant assigned;
    final GrantOwnership ownership;
    final WorkHandler handler;

    GrantInfo(
        Clock clock,
        Grant grant,
        LeaseState state,
        WorkHandler handler,
        ExecutorService handlerPool,
        Consumer<Update> updateFn,
        Supplier<Cluster> clusterSupplier) {
      this.id = grant.id();
      this.shard = grant.shard();
      this.state = state;
      this.assigned = grant.assigned();
      this.ownership =
          new GrantOwnership(
              clock,
              grant.id(),
              grant.lease(),
              grant.state(),
              handlerPool,
              updateFn,
              clusterSupplier);
      this.handler = handler;
    }

    Grant getGrant() {
      return Grant.create(id, shard, ownership.getState(), ownership.getExpiration(), assigned);
    }

    @Override
    public String toString() {
      return String.format(
          "grant_info{id=%s, shard=%s, state=%s, expiration=%s, lease=%s, assigned=%s}",
          id, shard, ownership.getState(), ownership.getExpiration(), state, assigned);
    }
  }

  private static class GrantOwnership implements Ownership {

    private final Clock clock;
    private final String grantId;
    private final ExecutorService handlerPool;
    private final SettableFuture<Void> terminated;
    private final Consumer<Update> updateFn;
    private final Supplier<Cluster> clusterSupplier;

    private Instant expiration;
    private GrantState state;
    private boolean otherLoaded;
    private boolean otherUnloaded;
    private boolean stopped;
    private final Object lock = new Object();

    public GrantOwnership(
        Clock clock,
        String grantId,
        Instant expiration,
        GrantState state,
        ExecutorService handlerPool,
        Consumer<Update> updateFn,
        Supplier<Cluster> clusterSupplier) {
      this.clock = clock;
      this.grantId = grantId;
      this.terminated = SettableFuture.create();
      this.updateFn = updateFn;
      this.clusterSupplier = clusterSupplier;
      this.handlerPool = handlerPool;
      this.expiration = expiration;
      this.state = state;
      this.otherLoaded = false;
      this.otherUnloaded = false;
      this.stopped = false;
    }

    @Override
    public void onTermination(Runnable r) {
      terminated.addListener(r, handlerPool);
    }

    @Override
    public Instant getExpiration() {
      synchronized (lock) {
        return expiration;
      }
    }

    @Override
    public GrantState getState() {
      synchronized (lock) {
        return state;
      }
    }

    @Override
    public boolean isTerminated() {
      synchronized (lock) {
        return stopped;
      }
    }

    public void setExpiration(Instant expiration) {
      synchronized (lock) {
        this.expiration = expiration;
      }
    }

    @Override
    public boolean waitForActive() {
      synchronized (lock) {
        while (state != GrantState.ACTIVE && !state.isInRevokedState() && !stopped) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return state == GrantState.ACTIVE;
      }
    }

    @Override
    public boolean waitForRevoked() {
      synchronized (lock) {
        while (!state.isInRevokedState() && !stopped) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return state.isInRevokedState();
      }
    }

    @Override
    public boolean waitForOtherGrantToUnload() {
      synchronized (lock) {
        while (state != GrantState.ACTIVE
            && !otherUnloaded
            && !state.isInRevokedState()
            && !stopped) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return state == GrantState.ACTIVE || otherUnloaded;
      }
    }

    @Override
    public boolean waitForOtherGrantToLoad() {
      synchronized (lock) {
        while (!otherLoaded && !stopped) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        return otherLoaded;
      }
    }

    @Override
    public void load() {
      boolean allocated;
      synchronized (lock) {
        if (stopped) {
          return;
        }
        allocated = state == GrantState.ALLOCATED;
        if (allocated) {
          state = GrantState.ALLOCATED_LOADED;
        }
      }
      if (allocated) {
        updateFn.accept(new Update(grantId, GrantState.ALLOCATED, GrantState.ALLOCATED_LOADED));
      }
    }

    @Override
    public void unload() {
      boolean revoked;
      synchronized (lock) {
        if (stopped) {
          return;
        }
        revoked = state == GrantState.REVOKED;
        if (revoked) {
          state = GrantState.REVOKED_UNLOADED;
        }
      }
      if (revoked) {
        updateFn.accept(new Update(grantId, GrantState.REVOKED, GrantState.REVOKED_UNLOADED));
      }
    }

    @Override
    public void release() {
      synchronized (lock) {
        if (stopped) {
          return;
        }
        stopped = true;
        lock.notifyAll();
      }
      handlerPool.submit(() -> terminated.set(null));
    }

    @Override
    public Cluster getCluster() {
      return clusterSupplier.get();
    }

    public void activate(Instant expiration) {
      synchronized (lock) {
        if (stopped) {
          return;
        }
        state = GrantState.ACTIVE;
        this.expiration = expiration;
        lock.notifyAll();
      }
    }

    public void revoke(Instant expiration) {
      synchronized (lock) {
        if (stopped) {
          return;
        }
        state = GrantState.REVOKED;
        this.expiration = expiration;
        lock.notifyAll();
      }
    }

    public void revokeIfNotRevoked() {
      synchronized (lock) {
        if (stopped || state.isInRevokedState()) {
          return;
        }
        state = GrantState.REVOKED;
        lock.notifyAll();
      }
    }

    public void otherGrantLoaded() {
      synchronized (lock) {
        otherLoaded = true;
        lock.notifyAll();
      }
    }

    public void otherGrantUnloaded() {
      synchronized (lock) {
        otherUnloaded = true;
        lock.notifyAll();
      }
    }

    private boolean isExpired() {
      return Instant.now(clock).isAfter(expiration);
    }

    void notifyLock() {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  record Update(String grantId, GrantState fromState, GrantState state) {}

  @VisibleForTesting
  static void notifyGrantLock(Ownership ownership) {
    if (ownership instanceof GrantOwnership) {
      ((GrantOwnership) ownership).notifyLock();
    }
  }
}
