package co.atoms.splitter.cluster;

import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Cluster provides access to information about all consumers and grants in the work distribution
 * process.
 */
public interface Cluster {
  record ConsumerGrant(Instance consumer, GrantInfo grant) {
    public static ConsumerGrant create(Instance consumer, GrantInfo grantInfo) {
      return new ConsumerGrant(consumer, grantInfo);
    }

    @Override
    public String toString() {
      return consumer() + "{" + grant() + "}";
    }
  }

  record ConsumerWithGrants(Instance consumer, ImmutableList<GrantInfo> grants) {
    public static ConsumerWithGrants create(Instance consumer, List<GrantInfo> grants) {
      return new ConsumerWithGrants(consumer, ImmutableList.copyOf(grants));
    }

    @Override
    public String toString() {
      return consumer() + "{" + grants() + "}";
    }
  }

  /** Cluster identification. */
  ClusterId getId();

  /**
   * Returns all the consumers joined the work distribution process (even without assigned grants)
   */
  List<Instance> getConsumers();

  /** Returns the consumer and grants for the given consumer id, if present. */
  Optional<Instance> getConsumer(String id);

  /** Returns consumer and grants for the given consumer id, if present. */
  Optional<ConsumerWithGrants> getConsumerGrants(String id);

  /** Returns consumer and grant for the given grant id, if present. */
  Optional<ConsumerGrant> getGrant(String id);

  /** Returns all shards in the cluster */
  Set<Shard> getShards();

  /** Returns all shards in the cluster for the given domain */
  List<Shard> getDomainShards(QualifiedDomainName domain);

  /**
   * Lookup returns the consumer and grant, if any, for the given key, with the constraint that the
   * state is the first present in the given list. If none are provided, lookup implicitly uses the
   * default notion of ownership under possible transitional states: [Active, Revoked, Loaded,
   * Unloaded].
   */
  Optional<ConsumerGrant> lookup(QualifiedDomainKey key, GrantState... states);
}
