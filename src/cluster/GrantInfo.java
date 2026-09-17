package co.atoms.splitter.cluster;

import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Shard;

/**
 * Contains information about a grant assigned to some consumer. It only contains information
 * relevant to the consumers not owning the grant (e.g. lease is excluded).
 *
 * @param id Unique grant ID
 * @param shard Shard assigned this grant. A shard can be assigned to at most two grants.
 * @param state State of the grant.
 */
public record GrantInfo(String id, Shard shard, GrantState state) {

  public static GrantInfo create(String id, Shard shard, GrantState state) {
    return new GrantInfo(id, shard, state);
  }

  public static GrantInfo fromProto(co.atoms.splitter.proto.ClusterMessage.GrantInfo info) {
    return create(
        info.getId(), Shard.fromProto(info.getShard()), GrantState.fromProto(info.getState()));
  }

  public co.atoms.splitter.proto.ClusterMessage.GrantInfo toProto() {
    return co.atoms.splitter.proto.ClusterMessage.GrantInfo.newBuilder()
        .setId(id())
        .setShard(shard().toProto())
        .setState(state().toProto())
        .build();
  }

  @Override
  public String toString() {
    return id() + "[shard=" + shard() + ", state=" + state() + "]";
  }
}
