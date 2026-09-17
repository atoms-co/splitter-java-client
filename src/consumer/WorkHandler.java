package co.atoms.splitter.consumer;

import co.atoms.splitter.model.Shard;

/** WorkHandler processes grants. Must be concurrency-safe. */
@FunctionalInterface
public interface WorkHandler {
  /**
   * Handle an assigned grant.
   *
   * @param id A unique id of a grant.
   * @param shard The shard of the assigned grant. Note that two grants maybe assigned to a single
   *     shard (allocated and revoked).
   * @param ownership Should be used to control ownership of the grant.
   */
  void handleWork(String id, Shard shard, Ownership ownership);
}
