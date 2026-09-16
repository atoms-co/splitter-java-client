package co.atoms.splitter.consumer.processor;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to retry an operation in case of ownership and other errors.
 *
 * <p>This can be used when forwarding requests to remote ranges with automatic retry on ownership
 * changes and on service-specific errors that happen during range movements.
 */
public class OwnershipRetryer {

  @FunctionalInterface
  public interface ExceptionFilter {
    /** Returns true if the exception should be retried. */
    boolean shouldRetry(Exception e);
  }

  @FunctionalInterface
  public interface ActionWithStreamObserver<T> {
    void run(StreamObserver<T> observer);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(OwnershipRetryer.class);

  private final Clock clock;
  private final Consumer<Duration> sleeper;
  private final Duration timeout;
  private final Duration interval;
  private final int limit;
  private final int backoffMultiplier;
  private final ExceptionFilter exceptionFilter;
  private final String tag;
  private final Function<String, RuntimeException> timeoutExceptionProducer;

  private OwnershipRetryer(
      Clock clock,
      Consumer<Duration> sleeper,
      Duration timeout,
      Duration interval,
      int limit,
      int backoffMultiplier,
      ExceptionFilter exceptionFilter,
      String tag,
      Function<String, RuntimeException> timeoutExceptionProducer) {
    this.clock = clock;
    this.sleeper = sleeper;
    this.timeout = timeout;
    this.interval = interval;
    this.backoffMultiplier = backoffMultiplier;
    this.limit = limit;
    this.exceptionFilter = exceptionFilter;
    this.tag = tag;
    this.timeoutExceptionProducer = timeoutExceptionProducer;
  }

  public <T> void retry(ActionWithStreamObserver<T> action, StreamObserver<T> observer) {
    Instant deadline = clock.instant().plus(this.timeout);
    var attempt = 0;
    var currentInterval = interval;
    while (clock.instant().isBefore(deadline) && attempt < this.limit) {
      var wrapped = new WrappingStreamObserver<>(observer);
      action.run(wrapped);

      var exception = wrapped.getException();
      if (exception.isEmpty()) {
        return;
      }

      var ex = exception.get();
      if (ex instanceof StatusRuntimeException e) {
        if (e.getStatus().getCode() != Code.OUT_OF_RANGE && !exceptionFilter.shouldRetry(e)) {
          LOGGER.error("{} failed", tag, e);
          observer.onError(e);
          return;
        }
      } else if (ex instanceof Exception e) {
        if (!exceptionFilter.shouldRetry(e)) {
          LOGGER.error("{} failed", tag, e);
          observer.onError(e);
          return;
        }
      } else {
        LOGGER.error("{} failed", tag, ex);
        observer.onError(ex);
        return;
      }

      LOGGER.info("Retrying {} after {}ms", tag, currentInterval.toMillis());

      long jitter = ThreadLocalRandom.current().nextLong(100);
      sleeper.accept(currentInterval.plus(Duration.ofMillis(jitter)));
      currentInterval = currentInterval.multipliedBy(backoffMultiplier);
      attempt++;
    }

    var msg =
        attempt >= this.limit
            ? "%s did not succeed after %d attempt(s)".formatted(tag, attempt)
            : "%s did not succeed within %s".formatted(tag, this.timeout);
    observer.onError(timeoutExceptionProducer.apply(msg));
  }

  public static class Builder {
    private final String tag;
    private final Duration timeout;
    private final Duration interval;

    private Clock clock = Clock.systemDefaultZone();
    private Consumer<Duration> sleeper = Uninterruptibles::sleepUninterruptibly;
    private int limit = Integer.MAX_VALUE;
    private int backoffMultiplier = 1;
    private ExceptionFilter errorFilter = e -> false;
    private Function<String, RuntimeException> timeoutExceptionProducer =
        msg -> Code.DEADLINE_EXCEEDED.toStatus().withDescription(msg).asRuntimeException();

    /**
     * Creates a new builder with the given timeout, interval, and tag.
     *
     * @param timeout the maximum time to wait for the operation to complete. Note that total time
     *     can exceed this duration if the operation exceeds the timeout.
     * @param interval the time to wait between retries.
     * @param tag a tag to identify the operation in logs.
     */
    public Builder(Duration timeout, Duration interval, String tag) {
      this.timeout = timeout;
      this.interval = interval;
      this.tag = tag;
    }

    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /** Sets a custom sleeper to be used for waiting between retries. */
    public Builder withSleeper(Consumer<Duration> sleeper) {
      this.sleeper = sleeper;
      return this;
    }

    /** Sets the maximum number of retries. */
    public Builder withLimit(int limit) {
      Preconditions.checkArgument(limit > 0, "Limit must be positive");
      this.limit = limit;
      return this;
    }

    /** Sets the backoff multiplier for the retry interval. */
    public Builder withBackoffMultiplier(int backoffMultiplier) {
      this.backoffMultiplier = backoffMultiplier;
      return this;
    }

    /**
     * Sets an error handler. Default error handler is no-op.
     *
     * <p>Note that this hook is not called on ownership errors.
     */
    public Builder withExceptionFilter(ExceptionFilter exceptionFilter) {
      this.errorFilter = exceptionFilter;
      return this;
    }

    /**
     * Sets a custom exception producer for timeout exceptions. Default producer creates a {@link
     * StatusRuntimeException} with {@link Code#DEADLINE_EXCEEDED}.
     */
    public Builder withTimeoutExceptionProducer(
        Function<String, RuntimeException> timeoutExceptionProducer) {
      this.timeoutExceptionProducer = timeoutExceptionProducer;
      return this;
    }

    public OwnershipRetryer build() {
      return new OwnershipRetryer(
          clock,
          sleeper,
          timeout,
          interval,
          limit,
          backoffMultiplier,
          errorFilter,
          tag,
          timeoutExceptionProducer);
    }
  }

  public static OwnershipRetryer create(Duration timeout, Duration interval, String tag) {
    return new Builder(timeout, interval, tag).build();
  }

  private static class WrappingStreamObserver<RESP> implements StreamObserver<RESP> {

    private final StreamObserver<RESP> delegate;
    private final AtomicReference<Throwable> exception = new AtomicReference<>();

    private WrappingStreamObserver(StreamObserver<RESP> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onNext(RESP value) {
      delegate.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      exception.set(t);
    }

    @Override
    public void onCompleted() {
      delegate.onCompleted();
    }

    public Optional<Throwable> getException() {
      return Optional.ofNullable(exception.get());
    }
  }
}
