package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.consumer.WorkHandler;
import co.atoms.splitter.model.Shard;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link WorkHandler} that supports {@link Range} loading and unloading. */
public class LoadingWorkProcessor<T extends Range> extends BaseWorkProcessor<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadingWorkProcessor.class);

  private final Clock clock;
  private final Range.Factory<T> rangeFactory;

  public LoadingWorkProcessor(Range.Factory<T> rangeFactory) {
    this(Clock.systemDefaultZone(), rangeFactory);
  }

  public LoadingWorkProcessor(Clock clock, Range.Factory<T> rangeFactory) {
    this.clock = clock;
    this.rangeFactory = rangeFactory;
  }

  @Override
  public void handleWork(String id, Shard shard, Ownership ownership) {
    LOGGER.info("Handling work for {}", grantInfo(id, shard, ownership));
    if (!ownership.waitForOtherGrantToUnload()) {
      LOGGER.error(
          "Failed to wait for other grant to unload for {}", grantInfo(id, shard, ownership));
      return;
    }

    LOGGER.info("Loading work for {}", grantInfo(id, shard, ownership));

    var range = rangeFactory.create(id, shard, ownership);
    ownership.onTermination(range::terminateAsync);

    try {
      getRanges().allocated(id, shard, range);

      range.initialize();
      if (ownership.isTerminated()) {
        LOGGER.error("Range for {} is terminated while loading", grantInfo(id, shard, ownership));
        return;
      }

      ownership.load();
      LOGGER.info("Loaded work for {}", grantInfo(id, shard, ownership));

      getRanges().loaded(id, shard, range);

      LOGGER.info("Waiting for {} to be active", grantInfo(id, shard, ownership));
      if (!ownership.waitForActive()) {
        LOGGER.error("Failed to activate {}}", grantInfo(id, shard, ownership));
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
          "Grant {} is revoked. Unloading. Lease ends in {}",
          grantInfo(id, shard, ownership),
          timeout);

      getRanges().revoke(id, shard, range);
      range.drain(timeout);

      if (ownership.isTerminated()) {
        LOGGER.error("Range for {} is terminated while unloading", grantInfo(id, shard, ownership));
        return;
      }

      ownership.unload();
      getRanges().unloaded(id, shard, range);

      LOGGER.info(
          "Grant {} is unloaded, waiting for other grant to load", grantInfo(id, shard, ownership));
      if (!ownership.waitForOtherGrantToLoad()) {
        LOGGER.error(
            "Failed to wait for other grant to load for {}", grantInfo(id, shard, ownership));
        return;
      }

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
