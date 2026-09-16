package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.consumer.WorkHandler;
import co.atoms.splitter.model.Shard;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link WorkHandler} that supports basic grant handling, without loading and
 * unloading.
 */
public class WorkProcessor<T extends Range> extends BaseWorkProcessor<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkProcessor.class);

  private final Clock clock;
  private final Range.Factory<T> rangeFactory;

  public WorkProcessor(Range.Factory<T> rangeFactory) {
    this(Clock.systemDefaultZone(), rangeFactory);
  }

  public WorkProcessor(Clock clock, Range.Factory<T> rangeFactory) {
    this.clock = clock;
    this.rangeFactory = rangeFactory;
  }

  @Override
  public void handleWork(String id, Shard shard, Ownership ownership) {
    LOGGER.info("Handling work for {}", grantInfo(id, shard, ownership));

    var range = rangeFactory.create(id, shard, ownership);
    ownership.onTermination(range::terminateAsync);

    getRanges().allocated(id, shard, range);

    try {
      range.initialize();

      LOGGER.info("Waiting for {} to be active", grantInfo(id, shard, ownership));
      if (!ownership.waitForActive()) {
        LOGGER.error("Failed to activate {}", grantInfo(id, shard, ownership));
        return;
      }

      range.activateAsync();
      getRanges().activate(id, shard, range);

      LOGGER.info("Grant {} is active", grantInfo(id, shard, ownership));

      if (!ownership.waitForRevoked()) {
        LOGGER.error("Failed to wait for revoked {}", grantInfo(id, shard, ownership));
        return;
      }

      Duration timeout = Duration.between(this.clock.instant(), ownership.getExpiration());
      LOGGER.info(
          "Grant {} is revoked. Draining. Lease ends in {}",
          grantInfo(id, shard, ownership),
          timeout);

      getRanges().revoke(id, shard, range);
      range.drain(timeout);

      LOGGER.info("Finished handling {}", grantInfo(id, shard, ownership));
    } finally {
      getRanges().delete(id, shard);
      range.terminateAsync();
    }
  }

  private String grantInfo(String id, Shard shard, Ownership ownership) {
    return "grant{id=%s, shard=%s, expiration=%s, state=%s, terminated=%s}"
        .formatted(
            id, shard, ownership.getExpiration(), ownership.getState(), ownership.isTerminated());
  }
}
