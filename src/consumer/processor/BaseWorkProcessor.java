package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.cluster.GrantMap;
import co.atoms.splitter.consumer.WorkHandler;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import java.util.Optional;

/**
 * Implementation of {@link WorkHandler} with common logic to manage information about owned grants.
 */
public abstract class BaseWorkProcessor<R extends Range> implements WorkHandler, GrantManager<R> {

  private final GrantMap<R> ranges = new GrantMap<>();

  @Override
  public Optional<R> lookup(QualifiedDomainKey key, GrantState... states) {
    return ranges.lookup(key, states);
  }

  /**
   * Returns the ranges managed by this processor. Concrete implementations should use it to update
   * state of the grants.
   */
  protected GrantMap<R> getRanges() {
    return ranges;
  }

  @Override
  public String toString() {
    return "ranges=%s".formatted(ranges);
  }
}
