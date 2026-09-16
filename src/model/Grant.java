package co.atoms.splitter.model;

import co.atoms.splitter.utils.TimeUtils;
import java.time.Instant;

/**
 * Grant represents a shard with a lease (expiration time) and a state.
 *
 * <p>There could be multiple leases for the same shard, but with different states.
 *
 * @param id The unique identifier of the grant. The identifier is randomly generated and its
 *     uniqueness is enforced for a service. Grant with a given id is always assigned to a single
 *     consumer and is never re-assigned to another consumer.
 * @param shard The shard that is being leased under this grant. The shard can be leased by multiple
 *     grants at the same time, but with different states.
 * @param state The state of the grant.
 * @param lease The expiration time of the grant. The lease is automatically extended by the system
 *     if the grant is not revoked before the grant expiration time.
 * @param assigned The time when the grant was assigned to a consumer.
 */
public record Grant(String id, Shard shard, GrantState state, Instant lease, Instant assigned) {

  public static Grant create(
      String id, Shard shard, GrantState state, Instant lease, Instant assigned) {
    return new Grant(id, shard, state, lease, assigned);
  }

  public Grant withState(GrantState state) {
    return create(id, shard, state, lease, assigned);
  }

  public static Grant fromProto(co.atoms.splitter.proto.Grant grant) {
    return create(
        grant.getId(),
        Shard.fromProto(grant.getShard()),
        GrantState.fromProto(grant.getState()),
        TimeUtils.toInstant(grant.getLease()),
        TimeUtils.toInstant(grant.getAssigned()));
  }

  public co.atoms.splitter.proto.Grant toProto() {
    return co.atoms.splitter.proto.Grant.newBuilder()
        .setId(id)
        .setShard(shard.toProto())
        .setState(state.toProto())
        .setLease(TimeUtils.toTimestamp(lease))
        .setAssigned(TimeUtils.toTimestamp(assigned))
        .build();
  }

  @Override
  public String toString() {
    return String.format(
        "grant{id=%s, shard=%s, state=%s, lease=%s, assigned=%s}",
        id, shard, state, lease, assigned);
  }
}
