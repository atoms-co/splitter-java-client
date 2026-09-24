package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.consumer.Consumer;
import co.atoms.splitter.consumer.processor.exceptions.GrpcExceptions;
import co.atoms.splitter.internal.Metrics;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainKey;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forwards gRPC requests to a consumer. The consumer can be local or remote.
 *
 * <p>The forwarder uses an instance of {@link GrantManager} to find whether the request owner is
 * local. When it is not, it uses the cluster information from Splitter consumer to find the appropriate
 * remote consumer and performs a remote call to that consumer.
 *
 * <p>If the forwarded request fails, the forwarder will retry the request, including the resolution
 * of the request owner.
 */
public class GrpcRequestForwarder<
    REQ, RESP, STUB extends AbstractBlockingStub<STUB>, R extends GrpcRequestHandler<REQ, RESP>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcRequestForwarder.class);

  public interface RemoteHandler<REQ, RESP, STUB extends AbstractBlockingStub<STUB>> {
    RESP handle(STUB stub, REQ request);
  }

  private final Consumer consumer;
  private final GrantManager<R> grantManager;
  private final GrpcStubsCache<STUB> stubs;
  private final RemoteHandler<REQ, RESP, STUB> remoteHandler;
  private final OwnershipRetryer retryer;
  private final Duration timeout;

  /**
   * Creates a new forwarder.
   *
   * @param consumer Splitter consumer
   * @param grantManager Grant manager, usually an instance of a subclass of {@link
   *     BaseWorkProcessor}
   * @param stubsCache Cache with stubs.
   * @param remoteHandler Invokes gRPC request
   * @param retryer Retryer to use when forwarding requests
   * @param timeout Timeout for one invocation of the request executed to a remote instance.
   */
  public GrpcRequestForwarder(
      Consumer consumer,
      GrantManager<R> grantManager,
      GrpcStubsCache<STUB> stubsCache,
      RemoteHandler<REQ, RESP, STUB> remoteHandler,
      OwnershipRetryer retryer,
      Duration timeout) {
    this.consumer = consumer;
    this.grantManager = grantManager;
    this.remoteHandler = remoteHandler;
    this.retryer = retryer;
    this.stubs = stubsCache;
    this.timeout = timeout;
  }

  /**
   * Creates a new forwarder.
   *
   * @param consumer Splitter consumer
   * @param grantManager Grant manager, usually an instance of a subclass of {@link
   *     BaseWorkProcessor}
   * @param stubFactory Creates a new gRPC stub. The created stubs are cached using {@link
   *     GrpcStubsCache}.
   * @param remoteHandler Invokes gRPC request
   * @param retryer Retryer to use when forwarding requests
   * @param timeout Timeout for one invocation of the request executed to a remote instance.
   */
  public GrpcRequestForwarder(
      Consumer consumer,
      GrantManager<R> grantManager,
      Function<ManagedChannel, STUB> stubFactory,
      RemoteHandler<REQ, RESP, STUB> remoteHandler,
      OwnershipRetryer retryer,
      Duration timeout) {
    this(consumer, grantManager, new GrpcStubsCache<STUB>(stubFactory), remoteHandler, retryer, timeout);
  }

  /**
   * Creates a new forwarder with a default deadline for requests of one minute.
   *
   * @param consumer Splitter consumer
   * @param grantManager Grant manager, usually an instance of a subclass of {@link
   *     BaseWorkProcessor}
   * @param stubFactory Creates a new gRPC stub. The created stubs are cached using {@link
   *     GrpcStubsCache}.
   * @param remoteHandler Invokes gRPC request
   * @param retryer Retryer to use when forwarding requests
   */
  public GrpcRequestForwarder(
      Consumer consumer,
      GrantManager<R> grantManager,
      Function<ManagedChannel, STUB> stubFactory,
      RemoteHandler<REQ, RESP, STUB> remoteHandler,
      OwnershipRetryer retryer) {
    this(consumer, grantManager, stubFactory, remoteHandler, retryer, Duration.ofMinutes(1));
  }

  private static final GrantState[] EMPTY_GRANT_STATES = new GrantState[0];

  /**
   * Forwards the request either for local handling or to a remote instance.
   *
   * @param key Key to forward the request to
   * @param req Request to forward
   * @param respObserver Observer to handle response. Used for remote requests only.
   */
  public void forward(QualifiedDomainKey key, REQ req, StreamObserver<RESP> respObserver) {
    forward(key, req, respObserver, EMPTY_GRANT_STATES, EMPTY_GRANT_STATES);
  }

  /**
   * Forwards the request either for local handling or to a remote instance.
   *
   * @param key Key to forward the request to
   * @param req Request to forward
   * @param respObserver Observer to handle response. Used for remote requests only.
   * @param remoteLookupStates States to use when looking up a remote owner of the key. If empty,
   *     the default list of states is used (described in {@link
   *     co.atoms.splitter.cluster.GrantMap#lookup}
   * @param localLookupStates States to use when looking up a local owner of the key. If empty, the
   *     default list of states is used (described in {@link co.atoms.splitter.cluster.GrantMap#lookup}
   */
  public void forward(
      QualifiedDomainKey key,
      REQ req,
      StreamObserver<RESP> respObserver,
      GrantState[] remoteLookupStates,
      GrantState[] localLookupStates) {
    retryer.retry(
        o -> forwardOnce(key, req, o, remoteLookupStates, localLookupStates), respObserver);
  }

  private void forwardOnce(
      QualifiedDomainKey key,
      REQ req,
      StreamObserver<RESP> respObserver,
      GrantState[] remoteLookupStates,
      GrantState[] localLookupStates) {
    // Check if a grant is present locally to guard against a stale cluster map.
    // We have to be careful to not pick a non-owner based on resolution rules,
    // so we look up using ACTIVE only. Otherwise, an UNLOADED local range will
    // be picked over a remote LOADED, which is suboptimal.
    var result = grantManager.lookup(key, GrantState.ACTIVE);
    if (result.isPresent()) {
      Metrics.recordForwardedRequest(key.domain(), "local", "ok", Location.LOCAL);
      result.get().handle(req, respObserver);
      return;
    }

    var owner = consumer.getCluster().lookup(key, remoteLookupStates);
    if (owner.isEmpty()) {
      result = grantManager.lookup(key, localLookupStates);
      if (result.isPresent()) {
        Metrics.recordForwardedRequest(key.domain(), "local", "ok", Location.LOCAL);
        result.get().handle(req, respObserver);
        return;
      }

      LOGGER.error("No owner found for key {}. Local grants: {}", key, grantManager);
      Metrics.recordForwardedRequest(key.domain(), "unknown", "owner_not_found", Location.UNKNOWN);
      respObserver.onError(GrpcExceptions.outOfRange("No owner found for key %s".formatted(key)));
      return;
    }
    var consumer = owner.get().consumer();
    var stub = stubs.getStub(consumer);
    if (stub == null) {
      LOGGER.error("No stub found for instance {}", consumer);
      Metrics.recordForwardedRequest(key.domain(), "unknown", "owner_not_found", Location.UNKNOWN);
      respObserver.onError(
          GrpcExceptions.internal("No stub found for instance %s".formatted(consumer)));
      return;
    }
    Metrics.recordForwardedRequest(key.domain(), "remote", "ok", consumer.location());
    try {
      var resp =
          remoteHandler.handle(
              stub.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS), req);
      Metrics.recordHandledRequest(key.domain(), "remote", "ok", consumer.location());
      respObserver.onNext(resp);
      respObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      LOGGER.error("Request for key {} failed on remote instance {}", key, consumer, e);
      Metrics.recordHandledRequest(
          key.domain(),
          "remote",
          e.getStatus().getCode().name().toLowerCase(),
          consumer.location());
      respObserver.onError(e);
    }
  }
}
