package co.atoms.splitter.model;

/**
 * The state of a grant. The state can be one of the following (in lifecycle order):
 *
 * <ul>
 *   <li>ALLOCATED: The grant is allocated, but not activated. There could be another grant for the
 *       same shard with REVOKED state.
 *   <li>ALLOCATED_LOADED: The owner of ALLOCATED grant transitions it to ALLOCATED_LOADED to
 *       indicate the it acquired resources required to serve the shard (optional).
 *   <li>ACTIVE: ALLOCATION grant became active and is the only grant responsible for this shard.
 *       REVOKED grant for the same shard is deleted from the cluster.
 *   <li>REVOKED: The system is preparing to remove ownership of the shard. The owner must release
 *       the resources allocated for this shard as soon as possible.
 *   <li>REVOKED_UNLOADED: The owner of REVOKED grant transitions it to REVOKED_UNLOADED to indicate
 *       that it released resources of this shard for use by the owner of ALLOCATED shard
 *       (optional).
 * </ul>
 *
 * The grant transitions to the terminal state when it's removed from the cluster. It usually
 * happens after REVOKED state, but can also happen after ALLOCATED or ACTIVE states in edge cases.
 *
 * <p>The order of states in this enum is important. It is the natural order of the grant lifecycle
 * (excluding unknown).
 */
public enum GrantState {
  UNKNOWN,
  /**
   * ALLOCATED indicates the destination location for work movement, matching a REVOKED grant
   * (possibly unassigned).
   */
  ALLOCATED,
  /**
   * Sub-state to ALLOCATED that indicates a consumer has signaled that the grant has been Loaded.
   */
  ALLOCATED_LOADED,
  /** ACTIVE indicates work assigned to a single worker. No other grants for this work exist */
  ACTIVE,
  /**
   * REVOKED indicates the source location for work movement, matching an ALLOCATED grant (possibly
   * unassigned).
   */
  REVOKED,
  /**
   * Sub-state to REVOKED that indicates a consumer has signaled that the grant has been Unloaded.
   */
  REVOKED_UNLOADED;

  /** Returns true if the grant is in the ALLOCATED state or one of its sub-states. */
  public boolean isInAllocatedState() {
    return this == ALLOCATED || this == ALLOCATED_LOADED;
  }

  /** Returns true if the grant is in the REVOKED state or one of its sub-states. */
  public boolean isInRevokedState() {
    return this == REVOKED || this == REVOKED_UNLOADED;
  }

  public static GrantState fromProto(co.atoms.splitter.proto.GrantState state) {
    switch (state) {
      case ACTIVE:
        return ACTIVE;
      case ALLOCATED:
        return ALLOCATED;
      case REVOKED:
        return REVOKED;
      case ALLOCATED_LOADED:
        return ALLOCATED_LOADED;
      case REVOKED_UNLOADED:
        return REVOKED_UNLOADED;
      default:
        return UNKNOWN;
    }
  }

  public co.atoms.splitter.proto.GrantState toProto() {
    switch (this) {
      case ACTIVE:
        return co.atoms.splitter.proto.GrantState.ACTIVE;
      case ALLOCATED:
        return co.atoms.splitter.proto.GrantState.ALLOCATED;
      case REVOKED:
        return co.atoms.splitter.proto.GrantState.REVOKED;
      case ALLOCATED_LOADED:
        return co.atoms.splitter.proto.GrantState.ALLOCATED_LOADED;
      case REVOKED_UNLOADED:
        return co.atoms.splitter.proto.GrantState.REVOKED_UNLOADED;
      default:
        return co.atoms.splitter.proto.GrantState.UNKNOWN;
    }
  }

  /** Returns true if the grant can advance to the given state. */
  public boolean canAdvanceTo(GrantState next) {
    return switch (this) {
      case ALLOCATED -> next != ALLOCATED;
      case ALLOCATED_LOADED -> !next.isInAllocatedState();
      case ACTIVE -> next.isInRevokedState();
      case REVOKED -> next == REVOKED_UNLOADED;
      default -> false;
    };
  }
}
