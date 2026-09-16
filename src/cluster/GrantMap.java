package co.atoms.splitter.cluster;

import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * GrantMap is a map Grant -> T optimized for domain key lookup.
 *
 * <p>Safe for concurrent access.
 */
public class GrantMap<T> {
  private final Clock clock;
  private final ShardMap<String, GrantMapEntry<T>> grants;
  private volatile Instant lastUpdated;

  public GrantMap() {
    this(Clock.systemDefaultZone());
  }

  public GrantMap(Clock clock) {
    this.clock = clock;
    this.grants = new ShardMap<>();
    this.lastUpdated = Instant.now(clock);
  }

  /** Returns known domains in the map. */
  public List<QualifiedDomainName> getDomains() {
    return grants.getDomains();
  }

  /** Returns all known domain grants. */
  public List<T> getDomainGrants(QualifiedDomainName domain) {
    return grants.getShards(domain).stream()
        .map(kv -> kv.value().value())
        .collect(ImmutableList.toImmutableList());
  }

  /** Update shard grant to allocated state and with given value */
  public void allocated(String grant, Shard shard, T value) {
    write(grant, shard, GrantState.ALLOCATED, value);
  }

  /** Update shard grant to loaded state and with given value */
  public void loaded(String grant, Shard shard, T value) {
    write(grant, shard, GrantState.ALLOCATED_LOADED, value);
  }

  /** Update shard grant to active state and with given value */
  public void activate(String grant, Shard shard, T value) {
    write(grant, shard, GrantState.ACTIVE, value);
  }

  /** Update shard grant to revoked state and with given value */
  public void revoke(String grant, Shard shard, T value) {
    write(grant, shard, GrantState.REVOKED, value);
  }

  /** Update shard grant to unloaded state and with given value */
  public void unloaded(String grant, Shard shard, T value) {
    write(grant, shard, GrantState.REVOKED_UNLOADED, value);
  }

  /** Update shard grant to the given state and with given value */
  public void write(String grant, Shard shard, GrantState state, T value) {
    grants.write(shard, grant, new GrantMapEntry<>(state, value));
    lastUpdated = Instant.now(clock);
  }

  /** Delete shard grant */
  public void delete(String grant, Shard shard) {
    grants.delete(shard, grant);
    lastUpdated = Instant.now(clock);
  }

  public String toString() {
    return String.format("grantMap{grant=%s, lastUpdated=%s}", grants, lastUpdated);
  }

  /**
   * Lookup returns value, if any, for the given key, with the constraint that the state is the
   * first present in the given list. If none are provided, lookup implicitly uses the default
   * notion of ownership under possible transitional states: [Active, Revoked, Loaded, Unloaded].
   */
  public Optional<T> lookup(QualifiedDomainKey key, GrantState... states) {
    if (states.length == 0) {
      states = ClusterMap.LOOKUP_DEFAULT_GRANT_STATES;
    }

    var candidates = grants.lookup(key);
    for (var candidate : candidates) {
      for (var state : states) {
        if (candidate.value().state() == state) {
          return Optional.of(candidate.value().value());
        }
      }
    }
    return Optional.empty();
  }

  record GrantMapEntry<T>(GrantState state, T value) {}
}
