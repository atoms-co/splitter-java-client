package co.atoms.splitter.consumer;

import co.atoms.splitter.client.JoinClient;
import co.atoms.splitter.cluster.Cluster;
import co.atoms.splitter.cluster.ClusterId;
import co.atoms.splitter.cluster.ClusterMap;
import co.atoms.lib.net.session.Session;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.proto.ClientMessage;
import co.atoms.splitter.proto.ClusterMessage;
import co.atoms.splitter.proto.JoinMessage;
import co.atoms.splitter.utils.TimeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer manages communication with a coordinator through a {@link Session}. Grants assigned to
 * this consumer are managed by a {@link WorkPool}. The session can be terminated (e.g. during
 * reconnects); during that time the work pool continues to manage the grants.
 *
 * <p>For every assigned grant the consumer starts a new handler using a dedicated executor service.
 * The consumer also uses an internal executor service (with limited number of threads) to manage
 * the session and the work pool.
 */
@SuppressWarnings("UnstableApiUsage") // For RateLimiter
public class Consumer extends AbstractService {

  /** Version of the client library. Should be kept in sync with Go client version. */
  public static final String CLIENT_VERSION = "1.2.3";

  private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

  private final RateLimiter connectLimiter = RateLimiter.create(1);

  private final Clock clock;
  private final Instance instance;
  private final QualifiedServiceName service;
  private final JoinClient joinClient;
  private final ExecutorService internalPool = Executors.newCachedThreadPool();
  private final WorkPool workPool;
  private final BooleanSupplier verbose;
  private final Function<Stream<Grant>, JoinMessage> registerFactory;

  private @Nullable volatile Session<JoinMessage, JoinMessage> session = null;
  private ClusterMap cluster;
  private final Object lock = new Object();

  /**
   * Creates a new consumer with default values. It will use the local IP address, pod region and
   * pod hostname to register with the coordinator. The grant handlers will be started in a new
   * thread for each grant.
   *
   * @param service Splitter service of this consumer.
   * @param joinClient gRPC client to join the work distribution process.
   * @param handler Handler for the work assigned to this consumer.
   * @param port Port of the consumer. Will be used by other consumers to connect to this consumer.
   */
  public static Consumer create(
      QualifiedServiceName service, JoinClient joinClient, WorkHandler handler, int port) {
    return new ConsumerBuilder(service, joinClient, handler, port).build();
  }

  /**
   * Creates a new consumer with default values. It will use the local IP address, pod region and
   * pod name to register with the coordinator. The grant handlers will be started in a new thread
   * for each grant.
   *
   * @param clock Clock to use for time-related operations.
   * @param service Splitter service of this consumer.
   * @param instance Consumer identification information for the work distribution process.
   * @param joinClient gRPC client to join the work distribution process.
   * @param handler Handler for the work assigned to this consumer.
   * @param handlerPool Executor service to run the grant handlers.
   * @param scheduler Executor service for internal scheduling.
   */
  protected Consumer(
      Clock clock,
      QualifiedServiceName service,
      Instance instance,
      JoinClient joinClient,
      WorkHandler handler,
      ExecutorService handlerPool,
      ScheduledExecutorService scheduler,
      BooleanSupplier verbose,
      Function<Stream<Grant>, JoinMessage> registerFactory) {
    this.clock = clock;
    this.instance = instance;
    this.service = service;
    this.joinClient = joinClient;
    this.workPool =
        new WorkPool(
            clock,
            service,
            instance,
            this::trySend,
            handler,
            handlerPool,
            internalPool,
            scheduler,
            this::getCluster);
    this.cluster = new ClusterMap(ClusterId.DEFAULT, List.of());
    this.verbose = verbose;
    this.registerFactory = registerFactory;
  }

  /**
   * @return cluster information about all consumers who have joined the work distribution process
   *     and their grants.
   */
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * @return instance of the current consumer.
   */
  public Instance getInstance() {
    return instance;
  }

  /**
   * Start the consumer and initiate a new session to the service. The consumer will re-connect
   * automatically in case of session termination until {@link #stopAsync()} is called. No-op if the
   * consumer is already started or stopping.
   */
  @Override
  protected void doStart() {
    LOGGER.info(
        "Starting consumer {} to service {} using client with version {}",
        instance,
        service,
        CLIENT_VERSION);
    internalPool.submit(this::connectLoop);
    workPool.init();
  }

  private static final Set<State> CONNECTABLE_STATES =
      Sets.immutableEnumSet(
          // We can connect when we're starting for the first time...
          State.STARTING,
          // ...or reconnect when we're already running
          State.RUNNING);

  private void connectLoop() {
    while (CONNECTABLE_STATES.contains(state())) {
      connectLimiter.acquire();

      var session =
          Session.startAsync(
              clock,
              instance.toProto().getInstance(),
              internalPool,
              joinClient::join,
              Messages::unwrapSessionMessage,
              Messages::wrapSessionMessage,
              this.verbose,
              this::handleConsumerMessage);

      LOGGER.info("Started session {}", session);

      var grants = workPool.getStaleGrants();
      session.sendAsync(registerFactory.apply(grants));

      session.awaitEstablishedOrTerminated();

      if (session.isTerminated()) {
        LOGGER.info("Session {} is terminated before establishing. Reconnecting", session);
        continue;
      }

      this.session = session;

      // This loop can reconnect, but we only want to notify that we started once. We should only be
      // in the STARTING state once. If state == STOPPING, we've been told to stop while we were
      // starting. In this case, we'll announce that we've started, which should trigger
      // termination after start finishes.
      var state = state();
      if (state == State.STARTING || state == State.STOPPING) {
        notifyStarted();
      }

      session.awaitTerminated();
      LOGGER.info("Session {} is terminated. Reconnecting", session);

      workPool.disconnected();

      this.session = null;
    }
  }

  private void handleConsumerMessage(JoinMessage msg) {
    if (msg.getMsgCase() != JoinMessage.MsgCase.CONSUMER) {
      return;
    }
    var consumerMsg = msg.getConsumer();
    switch (consumerMsg.getMsgCase()) {
      case CLIENT:
        handleClientMessage(consumerMsg.getClient());
        break;
      case CLUSTER:
        handleClusterMessage(consumerMsg.getCluster());
        break;
      default:
        LOGGER.warn("Unexpected message: {}. Ignoring", consumerMsg);
    }
  }

  private void handleClientMessage(ClientMessage msg) {
    switch (msg.getMsgCase()) {
      case EXTEND:
        var ttl = TimeUtils.toInstant(msg.getExtend().getLease());
        workPool.extend(ttl);
        break;
      case ASSIGN:
        var assigned =
            msg.getAssign().getGrantsList().stream()
                .map(Grant::fromProto)
                .collect(ImmutableList.toImmutableList());
        workPool.assign(assigned);
        break;
      case PROMOTE:
        var promoted =
            msg.getPromote().getGrantsList().stream()
                .map(Grant::fromProto)
                .collect(ImmutableList.toImmutableList());
        workPool.promote(promoted);
        break;
      case REVOKE:
        var revoked =
            msg.getRevoke().getGrantsList().stream()
                .map(Grant::fromProto)
                .collect(ImmutableList.toImmutableList());
        workPool.revoke(revoked);
        break;
      case NOTIFY:
        var update = Grant.fromProto(msg.getNotify().getUpdate());
        var target = Grant.fromProto(msg.getNotify().getTarget());
        workPool.update(update, target);
        break;
      default:
        LOGGER.warn("Unexpected message: {}. Ignoring", msg);
    }
  }

  private void handleClusterMessage(ClusterMessage msg) {
    synchronized (lock) {
      try {
        this.cluster =
            this.cluster.update(co.atoms.splitter.cluster.Messages.ClusterMessage.fromProto(msg));
      } catch (Exception e) {
        LOGGER.error("Failed to update cluster with message {}. Ignoring the update.", msg, e);
      }
    }
  }

  /**
   * Start shutdown process. The coordinator will be notified about the shutdown. No new grants will
   * be assigned after that. No-op if the consumer is already stopping.
   */
  @Override
  protected void doStop() {
    internalPool.submit(() -> terminate(session));
  }

  private void terminate(@Nullable Session<JoinMessage, JoinMessage> session) {
    if (session != null) {
      sendDeregister(session);
    }
    workPool.stopAsync();
    if (session != null) {
      session.awaitTerminated();
    }
    try {
      workPool.awaitTerminated();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      notifyStopped();
    }
  }

  private void trySend(JoinMessage msg) {
    var session = this.session;
    if (session == null) {
      LOGGER.warn("No active session. Ignoring message: {}", msg);
      return;
    }
    session.sendAsync(msg);
  }

  private static void sendDeregister(Session<JoinMessage, JoinMessage> session) {
    session.sendAsync(Messages.createDeregister());
  }
}
