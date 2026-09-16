package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.cluster.GrantMap;
import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.consumer.processor.exceptions.GrpcExceptions;
import co.atoms.splitter.consumer.processor.exceptions.OwnerNotFoundException;
import co.atoms.splitter.internal.Metrics;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.Shard;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkProcessor that accepts gRPC requests and transforms {@link OwnerNotFoundException} to gRPC
 * errors.
 */
public class GrpcWorkProcessor<REQ, RESP, R extends Range & GrpcRequestHandler<REQ, RESP>>
    extends BaseWorkProcessor<R> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcWorkProcessor.class);

  private final BaseWorkProcessor<R> processor;

  public GrpcWorkProcessor(BaseWorkProcessor<R> processor) {
    this.processor = processor;
  }

  /**
   * Calls the given handler with the range that owns the given key.
   *
   * @throws OwnerNotFoundException if the key is not owned by any range
   */
  public void handleGrpcRequest(
      QualifiedDomainKey key, REQ req, StreamObserver<RESP> resp, GrantState... states) {
    var result = lookup(key, states);

    if (result.isEmpty()) {
      LOGGER.error("No owner found for key {}. Local grants: {}", key, processor);
      resp.onError(GrpcExceptions.outOfRange("No owner found for key %s".formatted(key)));
      return;
    }

    var handler = result.get();

    var localObserver = new LocalStreamObserver(resp, key);
    try {
      handler.handle(req, localObserver);
    } finally {
      if (!localObserver.recordedHandledRequest) {
        LOGGER.error("Request for key {} failed locally", key);
        Metrics.recordHandledRequest(key.domain(), "local", "error", Location.LOCAL);
      }
    }
  }

  @Override
  public Optional<R> lookup(QualifiedDomainKey key, GrantState... states) {
    return processor.lookup(key, states);
  }

  @Override
  protected GrantMap<R> getRanges() {
    return processor.getRanges();
  }

  @Override
  public void handleWork(String id, Shard shard, Ownership ownership) {
    processor.handleWork(id, shard, ownership);
  }

  private class LocalStreamObserver implements StreamObserver<RESP> {

    private final StreamObserver<RESP> delegate;
    private final QualifiedDomainKey key;
    volatile boolean recordedHandledRequest = false;

    private LocalStreamObserver(StreamObserver<RESP> delegate, QualifiedDomainKey key) {
      this.delegate = delegate;
      this.key = key;
    }

    @Override
    public void onNext(RESP value) {
      delegate.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("Request for key {} failed locally", key, t);
      if (t instanceof StatusRuntimeException e) {
        Metrics.recordHandledRequest(
            key.domain(), "local", e.getStatus().getCode().name().toLowerCase(), Location.LOCAL);
      } else {
        Metrics.recordHandledRequest(key.domain(), "local", "error", Location.LOCAL);
      }
      recordedHandledRequest = true;
      delegate.onError(t);
    }

    @Override
    public void onCompleted() {
      Metrics.recordHandledRequest(key.domain(), "local", "ok", Location.LOCAL);
      recordedHandledRequest = true;
      delegate.onCompleted();
    }
  }
}
