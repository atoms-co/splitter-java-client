package co.atoms.splitter.consumer.processor.testing;

import co.atoms.splitter.consumer.processor.GrpcHandlingRange;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.testing.Gate;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Optional;

public class TestRange extends GrpcHandlingRange<QualifiedDomainKey, QualifiedDomainKey> {
  public final Gate initialize = new Gate();
  public final Gate initialized = new Gate();
  public final Gate activate = new Gate();
  public final Gate activated = new Gate();
  public final Gate drain = new Gate();
  public final Gate drained = new Gate();
  public final Gate terminate = new Gate();
  public final Gate terminated = new Gate();
  public Optional<Duration> timeout = Optional.empty();

  @Override
  public void initialize() {
    initialize.open();
    initialized.await();
  }

  @Override
  public void activateAsync() {
    activate.open();
    activated.await();
  }

  @Override
  public void drain(Duration timeout) {
    this.timeout = Optional.of(timeout);
    drain.open();
    drained.await();
  }

  @Override
  public void terminateAsync() {
    terminate.open();
    terminated.await();
  }

  @Override
  public void handle(
      QualifiedDomainKey request, StreamObserver<QualifiedDomainKey> responseObserver) {
    responseObserver.onNext(request);
    responseObserver.onCompleted();
  }
}
