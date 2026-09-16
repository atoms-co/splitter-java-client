package co.atoms.splitter.consumer.testing;

import static co.atoms.splitter.testing.Asserts.assertElement;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.client.JoinClient;
import co.atoms.splitter.consumer.Consumer;
import co.atoms.splitter.consumer.Messages;
import co.atoms.splitter.consumer.WorkHandler;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.proto.JoinMessage;
import co.atoms.splitter.testing.Fixtures;
import co.atoms.splitter.testing.MoreMessages;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumer for unit tests. Simulates connection with a server and allows controlling the consumer
 * using server messages.
 */
public class TestConsumer extends Consumer {

  private static final co.atoms.lib.net.location.proto.Instance SERVER =
      Fixtures.newLocationInstance("test-server");

  private final Clock clock;
  private final Instance instance;
  private final QualifiedServiceName service;

  // Messages from the server to the consumer
  private final AtomicReference<StreamObserver<JoinMessage>> in;

  // Messages sent by the consumer to the server
  private final LinkedBlockingQueue<JoinMessage> sent;

  // Exceptions thrown by the consumer on the server stream
  private final LinkedBlockingQueue<Throwable> exceptions;

  // Indicates termination of the server stream
  private final LinkedBlockingQueue<Boolean> terminated;

  public TestConsumer(
      Clock clock,
      QualifiedServiceName service,
      Instance instance,
      JoinClient joinClient,
      WorkHandler handler,
      ExecutorService handlerPool,
      ScheduledExecutorService scheduler,
      LinkedBlockingQueue<JoinMessage> sent,
      LinkedBlockingQueue<Throwable> exceptions,
      LinkedBlockingQueue<Boolean> terminated,
      AtomicReference<StreamObserver<JoinMessage>> in) {
    super(
        clock,
        service,
        instance,
        joinClient,
        handler,
        handlerPool,
        scheduler,
        () -> false,
        grants ->
            Messages.createRegister(
                Messages.createRegisterBuilder(instance, service, grants).build()));
    this.clock = clock;
    this.instance = instance;
    this.service = service;
    this.sent = sent;
    this.exceptions = exceptions;
    this.terminated = terminated;
    this.in = in;
  }

  public static TestConsumer create(
      Clock clock,
      QualifiedServiceName service,
      Instance instance,
      WorkHandler handler,
      ExecutorService handlerPool,
      ScheduledExecutorService scheduler) {
    LinkedBlockingQueue<JoinMessage> sent = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Boolean> terminated = new LinkedBlockingQueue<>();
    var out = Fixtures.newStreamObserver(sent, exceptions, terminated);

    AtomicReference<StreamObserver<JoinMessage>> in = new AtomicReference<>();

    var joinClient =
        new JoinClient() {
          @Override
          public StreamObserver<JoinMessage> join(StreamObserver<JoinMessage> responseObserver) {
            in.set(responseObserver);
            return out;
          }
        };

    return new TestConsumer(
        clock,
        service,
        instance,
        joinClient,
        handler,
        handlerPool,
        scheduler,
        sent,
        exceptions,
        terminated,
        in);
  }

  public static TestConsumer create(
      Clock clock, QualifiedServiceName service, Instance instance, WorkHandler handler) {
    return create(
        clock,
        service,
        instance,
        handler,
        Executors.newCachedThreadPool(),
        Executors.newSingleThreadScheduledExecutor());
  }

  public static TestConsumer create(
      Clock clock, String name, QualifiedServiceName service, WorkHandler handler) {
    return create(
        clock,
        service,
        Fixtures.newInstance(name),
        handler,
        Executors.newCachedThreadPool(),
        Executors.newSingleThreadScheduledExecutor());
  }

  public static TestConsumer create(
      String name, QualifiedServiceName service, WorkHandler handler) {
    return create(
        Clock.systemDefaultZone(),
        service,
        Fixtures.newInstance(name),
        handler,
        Executors.newCachedThreadPool(),
        Executors.newSingleThreadScheduledExecutor());
  }

  public static TestConsumer create(QualifiedServiceName service, WorkHandler handler) {
    return create("test-consumer", service, handler);
  }

  /** Starts the consumer and initializes the connection with the server. */
  public void initialize(Instant lease) {
    startAsync();

    assertSentEstablish();
    sendEstablished();
    assertSentRegister();
    extend(lease);
  }

  /** Consumes the next message sent to the server and asserts that it is an establish message */
  public void assertSentEstablish() {
    var msg = assertElement(sent);
    assertTrue(msg.getSession().hasEstablish());
  }

  /** Sends Established message to the server */
  public void sendEstablished() {
    send(Messages.wrapSessionMessage(MoreMessages.newEstablished(clock, SERVER)));
  }

  /** Consumes the next message sent to the server and asserts that it is a register message */
  public void assertSentRegister(Grant... grants) {
    assertElement(
        sent,
        Messages.createRegister(
            Messages.createRegisterBuilder(instance, service, Arrays.stream(grants)).build()));
  }

  /** Terminated the server stream */
  public void terminateConnection() {
    var in = this.in.get();
    if (in == null) {
      throw new IllegalStateException("Not connected to the server");
    }
    in.onCompleted();
  }

  /** Asserts that the server stream is terminated */
  public void assertTerminated() {
    assertElement(terminated, true);
  }

  /** Asserts that the consumer has thrown an exception on the server stream */
  public void assertSessionException(Throwable e) {
    assertElement(exceptions, e);
  }

  /** Sends a message to the server. Throws exception if consumer is not connected to the server. */
  public void send(JoinMessage msg) {
    var in = this.in.get();
    if (in == null) {
      throw new IllegalStateException("No in stream observer");
    }
    in.onNext(msg);
  }

  /** Assigns grants to this consumer */
  public void assign(Grant... grants) {
    send(MoreMessages.newAssign(grants));
  }

  /** Notifies the consumer about a grant update */
  public void notifyGrant(Grant update, Grant target) {
    send(MoreMessages.newNotify(update, target));
  }

  /** Promotes grants to active */
  public void promote(Grant... grants) {
    send(MoreMessages.newPromote(grants));
  }

  /** Revokes grants from this consumer */
  public void revoke(Grant... grants) {
    send(MoreMessages.newRevoke(grants));
  }

  /** Extends the lease of the consumer. Causes extension of leases for all active grants. */
  public void extend(Instant lease) {
    send(MoreMessages.newExtends(lease));
  }

  /**
   * Signals the consumer to start shutdown process, verifies that the consumer has sent deregister
   * message and sends the final message to close server stream.
   */
  public void stop() {
    stopAsync();
    assertSentDeregistered();
    sendClosed();
  }

  /**
   * Consumes the next message sent to the server and asserts that it is a message to release some
   * grants
   */
  public void assertSentReleased(Grant... grants) {
    assertElement(sent, Messages.createReleased(grants));
  }

  /**
   * Consumes the next message sent to the server and asserts that it is a message to update a grant
   */
  public void assertSentUpdate(Grant grant) {
    assertElement(sent, Messages.createUpdate(grant));
  }

  /** Consumes the next message sent to the server and asserts that it is a message to deregister */
  public void assertSentDeregistered() {
    assertElement(sent, Messages.createDeregister());
  }

  /** Sends a message to the consumer to close the connection */
  public void sendClosed() {
    send(Messages.wrapSessionMessage(co.atoms.lib.net.session.Messages.createClosed("")));
  }

  /** Sends a message to the consumer with an initial cluster map */
  public void sendClusterSnapshot(
      List<Shard> shards, co.atoms.splitter.cluster.Messages.Assignment... assignments) {
    send(MoreMessages.newClusterSnapshot(SERVER, shards, assignments));
  }

  /** Sends a message to the consumer with an initial cluster map */
  public void sendClusterSnapshot(co.atoms.splitter.cluster.Messages.Assignment... assignments) {
    var shards = new ArrayList<Shard>();
    for (var assignment : assignments) {
      for (var grant : assignment.grants()) {
        shards.add(grant.shard());
      }
    }
    sendClusterSnapshot(shards, assignments);
  }

  /** Sends a message to the consumer with new assignments in the cluster map */
  public void sendClusterAssign(
      int version, co.atoms.splitter.cluster.Messages.Assignment... assignments) {
    send(MoreMessages.newClusterAssign(SERVER.getId(), version, assignments));
  }
}
