package co.atoms.splitter.cluster;

import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.utils.TimeUtils;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Messages that are used by Splitter to initialize and update the cluster information. */
public class Messages {

  /**
   * Assignment a list of grants assigned to a consumer. This can be a full or a partial list of
   * grants.
   */
  public record Assignment(Instance consumer, List<GrantInfo> grants) {

    public static Assignment create(Instance consumer, List<GrantInfo> grants) {
      return new Assignment(consumer, grants);
    }

    public static Assignment fromProto(
        co.atoms.splitter.proto.ClusterMessage.Assignment assignment) {
      List<GrantInfo> grants = new ArrayList<>(assignment.getGrantsCount());
      assignment.getGrantsList().forEach(grant -> grants.add(GrantInfo.fromProto(grant)));
      return create(Instance.fromProto(assignment.getConsumer()), grants);
    }

    public co.atoms.splitter.proto.ClusterMessage.Assignment toProto() {
      co.atoms.splitter.proto.ClusterMessage.Assignment.Builder builder =
          co.atoms.splitter.proto.ClusterMessage.Assignment.newBuilder();
      builder.setConsumer(consumer().toProto());
      grants().forEach(grant -> builder.addGrants(grant.toProto()));
      return builder.build();
    }
  }

  /**
   * Snapshot contains assignments for all known consumers, including consumers without grants.
   *
   * <p>This is the first message sent by Splitter to initialize the cluster.
   *
   * @param assignments List of assignments for some consumers.
   * @param origin Instance of the service component where the cluster map originates. Is used to
   *     validate that subsequent updates are coming from the same source.
   */
  public record Snapshot(
      List<Assignment> assignments,
      co.atoms.lib.net.location.proto.Instance origin,
      ImmutableList<Shard> shards) {

    public static Snapshot fromProto(co.atoms.splitter.proto.ClusterMessage.Snapshot snapshot) {
      List<Assignment> assignments = new ArrayList<>(snapshot.getAssignmentsCount());
      snapshot
          .getAssignmentsList()
          .forEach(assignment -> assignments.add(Assignment.fromProto(assignment)));
      var shards =
          snapshot.getShardsList().stream()
              .map(Shard::fromProto)
              .collect(ImmutableList.toImmutableList());
      return new Snapshot(assignments, snapshot.getOrigin(), shards);
    }
  }

  /**
   * Assign contains changes in grant ownership. The list of consumer grants is partial and only
   * contains assigned grants.
   */
  public record Assign(List<Assignment> assignments) {

    public static Assign create(List<Assignment> assignments) {
      return new Assign(assignments);
    }

    public static Assign fromProto(co.atoms.splitter.proto.ClusterMessage.Assign assign) {
      List<Assignment> assignments = new ArrayList<>(assign.getAssignmentsCount());
      assign
          .getAssignmentsList()
          .forEach(assignment -> assignments.add(Assignment.fromProto(assignment)));
      return create(assignments);
    }

    public co.atoms.splitter.proto.ClusterMessage.Assign toProto() {
      co.atoms.splitter.proto.ClusterMessage.Assign.Builder builder =
          co.atoms.splitter.proto.ClusterMessage.Assign.newBuilder();
      assignments().forEach(assignment -> builder.addAssignments(assignment.toProto()));
      return builder.build();
    }
  }

  /** Update indicates a change in a grant state. */
  public record Update(List<GrantInfo> grants) {
    public static Update fromProto(co.atoms.splitter.proto.ClusterMessage.Update update) {
      List<GrantInfo> grants = new ArrayList<>(update.getGrantsCount());
      update.getGrantsList().forEach(grant -> grants.add(GrantInfo.fromProto(grant)));
      return new Update(grants);
    }
  }

  /** Unassign signals that grants are not owned by consumers. */
  public record Unassign(List<String> grants) {
    public static Unassign fromProto(co.atoms.splitter.proto.ClusterMessage.Unassign unassign) {
      return new Unassign(unassign.getGrantsList());
    }
  }

  /** Remove signals that consumers have left the work distribution process. */
  public record Remove(List<String> consumers) {
    public static Remove fromProto(co.atoms.splitter.proto.ClusterMessage.Remove remove) {
      return new Remove(remove.getConsumersList());
    }
  }

  /** Shards indicates that the list of valid shards has changed. */
  public record Shards(ImmutableList<Shard> shards) {
    public static Shards fromProto(co.atoms.splitter.proto.ClusterMessage.Shards shards) {
      return new Shards(
          shards.getShardsList().stream()
              .map(Shard::fromProto)
              .collect(ImmutableList.toImmutableList()));
    }
  }

  /** Change is a series of modifications of a previously defined cluster. */
  public record Change(
      Optional<Assign> assign,
      Optional<Update> update,
      Optional<Unassign> unassign,
      Optional<Remove> remove,
      Optional<Shards> shards) {

    public static Change fromProto(co.atoms.splitter.proto.ClusterMessage.Change change) {
      Optional<Assign> assign =
          change.hasAssign() ? Optional.of(Assign.fromProto(change.getAssign())) : Optional.empty();
      Optional<Update> update =
          change.hasUpdate() ? Optional.of(Update.fromProto(change.getUpdate())) : Optional.empty();
      Optional<Unassign> unassign =
          change.hasUnassign()
              ? Optional.of(Unassign.fromProto(change.getUnassign()))
              : Optional.empty();
      Optional<Remove> remove =
          change.hasRemove() ? Optional.of(Remove.fromProto(change.getRemove())) : Optional.empty();
      Optional<Shards> shards =
          change.hasShards() ? Optional.of(Shards.fromProto(change.getShards())) : Optional.empty();
      return new Change(assign, update, unassign, remove, shards);
    }
  }

  /**
   * ClusterMessage represents an update in grant ownership in a cluster.
   *
   * @param snapshot Snapshot of the cluster map. Empty afterward.
   * @param change Change in the cluster map. Empty for the first message.
   * @param id Stable unique identifier of the service component that manages the cluster map. When
   *     re-connected to a new service instance, it sends a new snapshot with a new id. Cluster
   *     messages with unknown IDs do not modify the cluster map.
   * @param version Version of the cluster map. Used for incremental updates. Out-of-order updates
   *     are not applied.
   * @param timestamp Timestamp of the cluster message. Can be used for debugging.
   */
  public record ClusterMessage(
      Optional<Snapshot> snapshot,
      Optional<Change> change,
      String id,
      long version,
      Instant timestamp) {

    public static ClusterMessage fromProto(co.atoms.splitter.proto.ClusterMessage cluster) {
      Optional<Snapshot> snapshot =
          cluster.hasSnapshot()
              ? Optional.of(Snapshot.fromProto(cluster.getSnapshot()))
              : Optional.empty();
      Optional<Change> change =
          cluster.hasChange()
              ? Optional.of(Change.fromProto(cluster.getChange()))
              : Optional.empty();
      return new ClusterMessage(
          snapshot,
          change,
          cluster.getId(),
          cluster.getVersion(),
          TimeUtils.toInstant(cluster.getTimestamp()));
    }
  }
}
