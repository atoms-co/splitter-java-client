package co.atoms.splitter.consumer;

import co.atoms.splitter.client.JoinClient;
import co.atoms.splitter.model.DomainKeyName;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.proto.ClientMessage;
import co.atoms.splitter.proto.JoinMessage;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A builder for creating a {@link Consumer}. */
public class ConsumerBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerBuilder.class);

  private final QualifiedServiceName service;
  private final Instance instance;
  private final JoinClient joinClient;
  private final WorkHandler handler;

  private @Nullable Clock clock;
  private @Nullable ExecutorService handlerPool;
  private @Nullable ScheduledExecutorService scheduler;
  private @Nullable Integer capacityLimit;
  private @Nullable DomainKeyName[] keyNames;
  private @Nullable BooleanSupplier verbose;

  /**
   * Creates a new builder for a {@link Consumer} with default values. It will use the local IP
   * address, pod region and pod hostname to register with the coordinator. The grant handlers will
   * be started in a new thread for each grant.
   *
   * @param service Splitter service name
   * @param joinClient Splitter join client
   * @param handler Splitter work handler, usually an instance of {@link
   *     co.atoms.splitter.consumer.processor.WorkProcessor} or {@link
   *     co.atoms.splitter.consumer.processor.LoadingWorkProcessor}
   * @param port port this consumer listens on. For services that use public and private APIs, this
   *     is the port for private API.
   */
  public ConsumerBuilder(
      QualifiedServiceName service, JoinClient joinClient, WorkHandler handler, int port) {
    this(service, joinClient, handler, defaultInstance(port));
  }

  /**
   * Creates a new builder for a {@link Consumer} with a given instance. Clients should use {@link
   * #ConsumerBuilder(QualifiedServiceName, JoinClient, WorkHandler, int)} instead - it creates a
   * new instance configured with proper information about the instance.
   */
  public ConsumerBuilder(
      QualifiedServiceName service, JoinClient joinClient, WorkHandler handler, Instance instance) {
    this.service = service;
    this.joinClient = joinClient;
    this.handler = handler;
    this.instance = instance;
  }

  /**
   * Sets the clock used by the consumer. If not set, the system default clock will be used.
   *
   * @return the builder with the clock set
   */
  public ConsumerBuilder withClock(Clock clock) {
    this.clock = clock;
    return this;
  }

  /**
   * Sets the pool used to for grant handlers. If not set, a new cached thread pool will be created.
   *
   * @return the builder with the handler pool set
   */
  public ConsumerBuilder withHandlerPool(ExecutorService handlerPool) {
    this.handlerPool = handlerPool;
    return this;
  }

  /**
   * Sets the scheduler used by the consumer. If not set, a new single-threaded scheduled executor
   * will be created.
   *
   * @return the builder with the scheduler set
   */
  public ConsumerBuilder withScheduler(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    return this;
  }

  /**
   * Sets the capacity limit of the consumer.
   *
   * @return the builder with the capacity set
   */
  public ConsumerBuilder withCapacityLimit(int capacity) {
    this.capacityLimit = capacity;
    return this;
  }

  /**
   * Sets the keys of the consumer.
   *
   * @return the builder with the canary keys set
   */
  public ConsumerBuilder withKeys(DomainKeyName... names) {
    this.keyNames = names;
    return this;
  }

  /**
   * Sets the verbose logging flag. If not set, verbose logging will be disabled.
   *
   * @return the builder with the verbose logging flag set
   */
  public ConsumerBuilder withVerboseLogging(BooleanSupplier verbose) {
    this.verbose = verbose;
    return this;
  }

  public Consumer build() {
    var clock = this.clock;
    if (clock == null) {
      clock = Clock.systemDefaultZone();
    }
    var handlerPool = this.handlerPool;
    if (handlerPool == null) {
      handlerPool =
          Executors.newCachedThreadPool(
              new ThreadFactoryBuilder().setNameFormat("splitter-handler-%d").build());
    }
    var scheduler = this.scheduler;
    if (scheduler == null) {
      scheduler =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setNameFormat("splitter-scheduler-%d").build());
    }
    var verbose = this.verbose;
    if (verbose == null) {
      verbose = () -> false;
    }

    Function<Stream<Grant>, JoinMessage> registerFactory =
        (grants) -> {
          var register = Messages.createRegisterBuilder(instance, service, grants);
          if (capacityLimit == null && keyNames == null) {
            return Messages.createRegister(register.build());
          }
          var opts = ClientMessage.Register.Options.newBuilder();
          if (capacityLimit != null) {
            LOGGER.info("Consumer capacity limit: {}", capacityLimit);
            opts = opts.setCapacityLimit(capacityLimit);
          }
          if (keyNames != null) {
            LOGGER.info(
                "Consumer key names: {}",
                Stream.of(keyNames).map(Object::toString).collect(Collectors.joining(", ")));
            opts = opts.addAllNames(Stream.of(keyNames).map(DomainKeyName::toProto).toList());
          }
          register = register.setOptions(opts.build());
          return Messages.createRegister(register.build());
        };

    return new Consumer(
        clock,
        service,
        instance,
        joinClient,
        handler,
        handlerPool,
        scheduler,
        verbose,
        registerFactory);
  }

  static Instance defaultInstance(int port) {
    return Instance.create(
        UUID.randomUUID().toString(),
        Location.LOCAL,
        Instant.now(),
        hostname(),
        String.format("%s:%s", resolveIP(), port));
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private static String resolveIP() {
    try {
      InetAddress ip = InetAddress.getLocalHost();
      return ip.getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
}
