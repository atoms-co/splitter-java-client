package co.atoms.splitter.cluster;

import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Cluster}. Allows to create a new cluster by applying a {@link
 * Messages.ClusterMessage}.
 */
public class ClusterMap implements Cluster {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterMap.class);

  static final GrantState[] LOOKUP_DEFAULT_GRANT_STATES =
      new GrantState[] {
        GrantState.ACTIVE,
        GrantState.REVOKED,
        GrantState.ALLOCATED_LOADED,
        GrantState.REVOKED_UNLOADED
      };

  private final ClusterId id;
  private final Map<String, consumerInfo> consumers;
  private final Map<String, grantInfo> grants;
  private final ImmutableSet<Shard> shards;
  private final ShardMap<String, ConsumerGrant> cache;

  public ClusterMap(ClusterId id, Collection<Shard> shards) {
    this.id = id;
    this.consumers = new HashMap<>();
    this.grants = new HashMap<>();
    this.shards = ImmutableSet.copyOf(shards);
    this.cache = new ShardMap<>();
  }

  @Override
  public ClusterId getId() {
    return id;
  }

  @Override
  public List<Instance> getConsumers() {
    return consumers.values().stream()
        .map(consumerInfo::consumer)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public Optional<Instance> getConsumer(String id) {
    return Optional.ofNullable(consumers.get(id)).map(consumerInfo::consumer);
  }

  @Override
  public Optional<ConsumerWithGrants> getConsumerGrants(String id) {
    return Optional.ofNullable(consumers.get(id)).map(consumerInfo::consumerWithGrants);
  }

  @Override
  public Optional<ConsumerGrant> getGrant(String id) {
    return Optional.ofNullable(grants.get(id)).map(grantInfo::consumerGrant);
  }

  @Override
  public Set<Shard> getShards() {
    return shards;
  }

  @Override
  public List<Shard> getDomainShards(QualifiedDomainName domain) {
    return shards.stream()
        .filter(shard -> shard.domain().equals(domain))
        .collect(ImmutableList.toImmutableList());
  }

  public Optional<List<String>> getShardGrants(Shard shard) {
    if (!shards.contains(shard)) {
      return Optional.empty();
    }
    return Optional.of(
        grants.values().stream()
            .filter(i -> i.grant().shard().equals(shard))
            .map(g -> g.grant().id())
            .toList());
  }

  public List<Messages.Assignment> getAssignments() {
    return consumers.values().stream()
        .map(c -> Messages.Assignment.create(c.consumer(), c.grants()))
        .toList();
  }

  @VisibleForTesting
  public Optional<Long> getGrantRetainedVersion(String grantId) {
    return Optional.ofNullable(grants.get(grantId)).map(grantInfo::version);
  }

  @VisibleForTesting
  public Optional<Long> getConsumerRetainedVersion(String consumerId) {
    return Optional.ofNullable(consumers.get(consumerId)).map(consumerInfo::version);
  }

  @Override
  public Optional<ConsumerGrant> lookup(QualifiedDomainKey key, GrantState... states) {
    if (states.length == 0) {
      states = LOOKUP_DEFAULT_GRANT_STATES;
    }

    Map<GrantState, ConsumerGrant> byState =
        cache.lookup(key).stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    s -> s.value().grant().state(), ShardMap.ShardKV::value));
    for (GrantState state : states) {
      ConsumerGrant grant = byState.get(state);
      if (grant != null) {
        return Optional.of(grant);
      }
    }
    return Optional.empty();
  }

  /**
   * Creates a new cluster by applying the given message. The message can be a snapshot or an
   * incremental update.
   *
   * @throws IllegalArgumentException if the message is not supported or cluster id and/or version
   *     are not as expected.
   */
  public ClusterMap update(Messages.ClusterMessage msg) {
    var timestamp = msg.timestamp();

    if (msg.snapshot().isPresent()) {
      var snapshot = msg.snapshot().get();
      var error = validateSnapshot(snapshot.shards(), snapshot.assignments());
      if (error != null) {
        throw new IllegalArgumentException("invalid cluster snapshot: %s".formatted(error));
      }
      var cluster =
          new ClusterMap(
              ClusterId.create(snapshot.origin(), msg.version(), timestamp), snapshot.shards());
      cluster.initAssignments(snapshot.assignments());

      var shardsWithGrants = new ShardsWithGrants(cluster.shards, cluster.grants.values());

      // Copy old assignments

      for (var g : grants.values()) {
        if (cluster.grants.containsKey(g.grant().id())) {
          continue;
        }

        if (g.version == 0) {
          g = new grantInfo(g.consumerGrant(), id.version());
        }

        var consumerVersion = Preconditions.checkNotNull(consumers.get(g.consumer().id())).version;
        if (consumerVersion == 0) {
          consumerVersion = id.version();
        }

        error = cluster.tryAssign(g, consumerVersion, shardsWithGrants);
        if (error != null) {
          LOG.warn(
              "Old grant is no longer valid in a new cluster map. Discarding. Grant: {}. Reason:"
                  + " {}",
              g.grant(),
              error);
          continue;
        }
      }

      return cluster;
    }

    if (msg.change().isPresent()) {
      if (!getId().isNext(msg.id(), msg.version())) {
        var cid = getId();
        throw new IllegalArgumentException(
            "Unexpected incremental update for cluster{origin=%s, version=%s}: %s v%s"
                .formatted(cid.origin().getId(), cid.version(), msg.id(), msg.version()));
      }

      var change = msg.change().get();
      Collection<Shard> shards = this.shards;
      if (change.shards().isPresent()) {
        shards = change.shards().get().shards();
      }

      List<Messages.Assignment> assigned =
          change.assign().stream().map(Messages.Assign::assignments).flatMap(List::stream).toList();
      Map<String, GrantInfo> updated =
          change.update().stream()
              .map(Messages.Update::grants)
              .flatMap(List::stream)
              .collect(ImmutableMap.toImmutableMap(GrantInfo::id, g -> g));
      Set<String> unassigned =
          change.unassign().stream()
              .map(Messages.Unassign::grants)
              .flatMap(List::stream)
              .collect(ImmutableSet.toImmutableSet());
      Set<String> removed =
          change.remove().stream()
              .map(Messages.Remove::consumers)
              .flatMap(List::stream)
              .collect(ImmutableSet.toImmutableSet());

      var error = validateUpdate(shards, assigned, updated, unassigned, removed);
      if (error != null) {
        throw new IllegalArgumentException("invalid cluster change: %s".formatted(error));
      }

      // Create a new cluster with assigned grants.
      var cluster = new ClusterMap(getId().next(timestamp), shards);
      cluster.initAssignments(assigned);

      // Copy over updated grants

      for (var g : updated.values()) {
        if (grants.containsKey(g.id())) {
          var info = grants.get(g.id());
          var grant = new grantInfo(ConsumerGrant.create(info.consumer(), g), 0);
          cluster.assign(grant, 0);
        }
      }

      var shardsWithGrants = new ShardsWithGrants(cluster.shards, cluster.grants.values());

      // Copy over retained values
      for (var entry : consumers.entrySet()) {
        var cid = entry.getKey();

        // Skip removed consumers.
        if (removed.contains(cid)) {
          continue;
        }

        // Ensure the consumer is copied even if it has no grants. Note that snapshot will remove
        // old consumers without grants.
        if (!cluster.consumers.containsKey(cid)) {
          cluster.consumers.put(
              cid,
              new consumerInfo(
                  new ConsumerWithGrants(entry.getValue().consumer(), ImmutableList.of()),
                  entry.getValue().version()));
        }

        for (var grant : entry.getValue().grants()) {
          // Skip unassigned grants or known grants.
          if (unassigned.contains(grant.id()) || cluster.grants.containsKey(grant.id())) {
            continue;
          }

          var g = Preconditions.checkNotNull(grants.get(grant.id()));
          error = cluster.tryAssign(g, entry.getValue().version(), shardsWithGrants);
          if (error != null) {
            LOG.warn(
                "Old grant is no longer valid in a new cluster map. Discarding. Grant: {}. Reason:"
                    + " {}",
                g.grant(),
                error);
            continue;
          }
        }
      }

      return cluster;
    }
    throw new IllegalArgumentException("Unsupported message: " + msg);
  }

  private void initAssignments(List<Messages.Assignment> assignments) {
    assignments.forEach(
        assignment -> {
          var grants = assignment.grants();
          var consumer = assignment.consumer();

          this.consumers.put(
              consumer.id(), new consumerInfo(ConsumerWithGrants.create(consumer, grants), 0));
          for (var grant : grants) {
            var info = new grantInfo(ConsumerGrant.create(consumer, grant), 0);
            this.grants.put(grant.id(), info);
            this.cache.write(grant.shard(), grant.id(), info.consumerGrant());
          }
        });
  }

  private @Nullable String tryAssign(
      grantInfo grant, long consumerVersion, ShardsWithGrants shardsWithGrants) {
    var g = grant.grant();
    if (!shards.contains(g.shard())) {
      return "unknown shard";
    }

    var error =
        shardsWithGrants.tryAssign(id -> Preconditions.checkNotNull(grants.get(id)).grant(), g);
    if (error != null) {
      return error;
    }

    assign(grant, consumerVersion);
    return null;
  }

  private void assign(grantInfo info, long consumerVersion) {
    var cid = info.consumer().id();
    var grant = info.grant();
    if (consumers.containsKey(cid)) {
      var consumer = consumers.get(cid);
      var newGrants =
          ImmutableList.<GrantInfo>builder().addAll(consumer.grants()).add(grant).build();
      consumers.put(
          cid,
          new consumerInfo(
              ConsumerWithGrants.create(consumer.consumer(), newGrants), consumer.version()));
    } else {
      consumers.put(
          cid,
          new consumerInfo(
              ConsumerWithGrants.create(info.consumer(), ImmutableList.of(grant)),
              consumerVersion));
    }

    var consumer = consumers.get(cid).consumer();
    info = new grantInfo(ConsumerGrant.create(consumer, grant), info.version());

    grants.put(grant.id(), info);
    this.cache.write(grant.shard(), grant.id(), info.consumerGrant());
  }

  private static @Nullable String validateSnapshot(
      Collection<Shard> shards, List<Messages.Assignment> assignments) {
    var error = validateShards(shards);
    if (error != null) {
      return error;
    }
    return validateAssignments(
        new ShardsWithGrants(shards, List.of()), new HashMap<>(), assignments);
  }

  private @Nullable String validateUpdate(
      Collection<Shard> shards,
      List<Messages.Assignment> assignments,
      Map<String, GrantInfo> updated,
      Set<String> unassigned,
      Set<String> removed) {
    var error = validateShards(shards);
    if (error != null) {
      return error;
    }

    ShardsWithGrants shardsWithGrants = new ShardsWithGrants(shards, List.of());

    // Copy existing grants for valid shards
    Map<String, GrantInfo> totalGrants = new HashMap<>();
    for (var entry : grants.entrySet()) {
      var g = entry.getValue();
      if (!shardsWithGrants.contains(g.grant().shard())) {
        // invalid shard, ignore grant
        continue;
      }
      if (g.isRetained()) {
        // Ignore grants from cluster map before the most recent snapshot
        continue;
      }
      totalGrants.put(g.grant().id(), g.grant());
    }

    // Updates should be sent for registered grants only
    for (var g : updated.values()) {
      if (!totalGrants.containsKey(g.id())) {
        return "updated unregistered grant %s".formatted(g.id());
      }
      error = validateGrantUpdate(totalGrants.get(g.id()), g);
      if (error != null) {
        return "invalid update for grant %s: %s".formatted(g.id(), error);
      }
      totalGrants.put(g.id(), g);
    }

    // Only registered grants should be unassigned
    for (var id : unassigned) {
      if (!totalGrants.containsKey(id)) {
        return "unassigned unregistered grant %s".formatted(id);
      }
      totalGrants.remove(id);
    }

    // Only registered consumers should be removed. Remove their grants too.
    for (var id : removed) {
      if (!consumers.containsKey(id) || consumers.get(id).isRetained()) {
        return "removed unregistered consumer %s".formatted(id);
      }
      consumers.get(id).grants().forEach(g -> totalGrants.remove(g.id()));
    }

    // Register old valid grants
    for (var g : totalGrants.values()) {
      shardsWithGrants.add(g);
    }

    // Pass registered grants, but not consumers. New assignments cannot contain registered grants,
    // but can contain registered consumers (with new, additional grants).
    return validateAssignments(shardsWithGrants, totalGrants, assignments);
  }

  private static @Nullable String validateShards(Collection<Shard> shards) {
    Set<Shard> s = new HashSet<>();
    for (var shard : shards) {
      if (!s.add(shard)) {
        return "duplicate shard %s".formatted(shard);
      }
    }
    return null;
  }

  private static @Nullable String validateGrantUpdate(GrantInfo old, GrantInfo updated) {
    if (!old.shard().equals(updated.shard())) {
      return "shard mismatch: %s != %s".formatted(old.shard(), updated.shard());
    }
    if (!old.state().canAdvanceTo(updated.state())) {
      return "state did not advance: %s >= %s".formatted(old.state(), updated.state());
    }
    return null;
  }

  private static @Nullable String validateAssignments(
      ShardsWithGrants shardsWithGrants,
      Map<String, GrantInfo> grants,
      List<Messages.Assignment> assignments) {
    var consumers = new HashSet<String>();

    for (var assignment : assignments) {
      var cid = assignment.consumer().id();

      // Consumer can only be listed once
      if (!consumers.add(cid)) {
        return "duplicate consumer %s".formatted(cid);
      }

      // Validate consumer grants
      for (var grant : assignment.grants()) {
        var error = validateGrant(shardsWithGrants, grants, grant);
        if (error != null) {
          return "consumer %s has invalid grant %s in assignments: %s"
              .formatted(cid, grant.id(), error);
        }
        grants.put(grant.id(), grant);
      }
    }
    return null;
  }

  private static @Nullable String validateGrant(
      ShardsWithGrants shardsWithGrants, Map<String, GrantInfo> grants, GrantInfo grant) {
    if (grants.containsKey(grant.id())) {
      return "duplicate grant";
    }
    // Shard should be valid
    if (!shardsWithGrants.contains(grant.shard())) {
      return "unknown shard %s".formatted(grant.shard());
    }

    // Check other grants assigned to the same shard
    return shardsWithGrants.tryAssign(grants::get, grant);
  }

  private record consumerInfo(ConsumerWithGrants consumerWithGrants, long version) {
    Instance consumer() {
      return consumerWithGrants.consumer();
    }

    ImmutableList<GrantInfo> grants() {
      return consumerWithGrants.grants();
    }

    // Retained returns true if the consumer was copied from an old cluster map during applying a
    // snapshot operation, and it was not listed by the coordinator since then.
    boolean isRetained() {
      return version > 0;
    }
  }

  private record grantInfo(ConsumerGrant consumerGrant, long version) {
    Instance consumer() {
      return consumerGrant.consumer();
    }

    GrantInfo grant() {
      return consumerGrant.grant();
    }

    // Returns true if the grant was copied from an old cluster map during applying a snapshot
    // operation, and it was not listed by the coordinator since then.
    boolean isRetained() {
      return version > 0;
    }
  }

  private static class ShardsWithGrants {
    private final Map<Shard, Set<String>> shardsWithGrants = new HashMap<>();

    ShardsWithGrants(Collection<Shard> shards, Collection<grantInfo> grants) {
      shards.forEach(s -> shardsWithGrants.put(s, new HashSet<>()));
      grants.forEach(g -> add(g.grant()));
    }

    Set<String> get(Shard shard) {
      return Preconditions.checkNotNull(shardsWithGrants.get(shard));
    }

    boolean contains(Shard shard) {
      return shardsWithGrants.containsKey(shard);
    }

    void add(GrantInfo g) {
      get(g.shard()).add(g.id());
    }

    /**
     * Checks whether a grant can be assigned to a shard and adds the grant to a list of grants
     * assigned to the shard.
     *
     * <p>A grant can be assigned to a shard if it's not conflicting with other grants assigned to
     * the shard.
     */
    @Nullable
    String tryAssign(Function<String, GrantInfo> grants, GrantInfo g) {
      var shardGrants = get(g.shard());
      switch (shardGrants.size()) {
        case 0:
          break;
        case 1:
          var otherGrantId = shardGrants.iterator().next();
          var otherGrant = grants.apply(otherGrantId);
          if (!grantStatesCompatible(otherGrant.state(), g.state())) {
            return "grant %s has conflicting state with another grant assigned to the same shard: %s"
                .formatted(g, otherGrant);
          }
          break;
        default:
          shardGrants.add(g.id());
          return "shard %s has too many assigned grants: %s"
              .formatted(
                  g.shard(),
                  shardGrants.stream().sorted().collect(Collectors.joining(" ", "[", "]")));
      }
      add(g);
      return null;
    }
  }

  /** Verifies that states of the given grant states are compatible for the same shard. */
  private static boolean grantStatesCompatible(GrantState state1, GrantState state2) {
    var allocated1 = state1.isInAllocatedState();
    var revoked1 = state1.isInRevokedState();
    var allocated2 = state2.isInAllocatedState();
    var revoked2 = state2.isInRevokedState();
    return (allocated1 && revoked2) || (allocated2 && revoked1);
  }

  @Override
  public String toString() {
    return "ClusterMap{id=%s, consumers=%s, grants=%s}".formatted(id, consumers, grants);
  }
}
