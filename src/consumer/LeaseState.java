package co.atoms.splitter.consumer;

/** Local state of a grant lease. */
enum LeaseState {
  /** The grant is allocated/active and under the (renewed) lease. */
  ACTIVE,

  /**
   * The grant has been revoked by the coordinator. Lease will not be renewed after expiration time.
   */
  REVOKED,

  /**
   * The grant is allocated/active, but will lapse due to a disconnect from the coordinator. Most
   * grants will be updated after re-connect, but that is the coordinator's decision.
   */
  STALE,
}
