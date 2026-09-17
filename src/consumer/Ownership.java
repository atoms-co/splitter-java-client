package co.atoms.splitter.consumer;

import co.atoms.splitter.cluster.Cluster;
import co.atoms.splitter.model.GrantState;
import java.time.Instant;

/**
 * Ownership holds information about the grant state and expiration, as well as signals for
 * participating in graceful transitions via load/unload. Ownership should be used to ensure correct
 * progression through the grant lifecycle.
 *
 * <p>Grant handlers that use graceful handover follow this sequence:
 *
 * <pre>
 * 	void handler(String id, Shard shard, o Ownership) {
 * 	  // wait for prior counterparty to be UNLOADED
 * 	  if (!o.waitForOtherGrantToUnload()) {
 * 	    return
 * 	  }
 * 	  o.Load()                               // ALLOCATED -> LOADED
 * 	  .. preferred part owner ..
 * 	  if (!o.waitForActive()) {              // LOADED    -> ACTIVE
 * 	    return
 * 	  }
 * 	  .. exclusive owner ..
 * 	  if (!o.waitForRevoked()) {             // ACTIVE    -> REVOKED
 * 	    return
 * 	  }
 * 	  o.Unload()                             // REVOKED   -> UNLOADED
 * 	  .. still part owner but not preferred ..
 * 	  // wait for next counterparty to be LOADED
 * 	  if (!o.waitForOtherGrantToLoad()) {
 * 	    return
 * 	  }
 * 	  // returning relinquishes the grant
 * 	}
 * 	</pre>
 *
 * <p>The load/unload aspects are optional. If not needed, the handler can be:
 *
 * <pre>
 * 	void handler(String id, Shard shard, o Ownership) {
 * 	  if (!o.waitForActive()) {              // ALLOCATED -> ACTIVE
 * 	    return
 * 	  }
 * 	  .. exclusive owner ..
 * 	  if (!o.waitForRevoked()) {             // ACTIVE    -> REVOKED
 * 	    return
 * 	  }
 * 	  // returning relinquishes the grant
 * 	}
 * 	</pre>
 *
 * <p>The waitFor methods ensure that if a step does not happen, such as a grant directly being
 * assigned in ACTIVE state, the logic progresses as expected. If the grant is unexpectedly lost,
 * the methods return false to let the handler exit.
 */
public interface Ownership {

  /**
   * Returns expiration of the grant.
   *
   * <p>The expiration is periodically updated when connected to the server. When grant is expired,
   * the grant is released.
   */
  Instant getExpiration();

  /** Returns current grant state */
  GrantState getState();

  /**
   * Returns true if the grant is released or expired. Handler must stop processing the grant if
   * it's terminated.
   */
  boolean isTerminated();

  /**
   * Wait for the counterpart of this grant to transition to Unloaded. It may never happen.
   * Typically, this signals that the handler can safely initialize and become ready to assume
   * ownership.
   *
   * <p>This call is blocking. In case there is an asynchronous error, the handler must call {@link
   * #release} to signal the release of the grant and unblock this call.
   *
   * @return true if the counterpart has been unloaded or this grant is active, false if the current
   *     grant is revoked, expired or terminated.
   */
  boolean waitForOtherGrantToUnload();

  /** Transition the grant from Allocated to Loaded. Has no effect if the grant is active. */
  void load();

  /**
   * Wait for the grant to transition to Active. It may never happen. This signals that the handler
   * can safely assume sole ownership of the shard.
   *
   * <p>This call is blocking. In case there is an asynchronous error, the handler must call {@link
   * #release} to signal the release of the grant and unblock this call.
   *
   * @return true if this grant is active, false if the grant is revoked, expired or terminated.
   */
  boolean waitForActive();

  /**
   * Wait for the grant to transition to Revoked. It may never happen. This signal implies that
   * there is another grant for the same shard with Allocated state.
   *
   * <p>This call is blocking. In case there is an asynchronous error, the handler must call {@link
   * #release} to signal the release of the grant and unblock this call.
   *
   * @return true if this grant is revoked, false if the grant is expired or terminated.
   */
  boolean waitForRevoked();

  /** Transition the grant from Revoked to Unloaded. */
  void unload();

  /**
   * Wait for the counterpart to transition to Loaded, usually in response to the Unload signal. It
   * may never happen.
   *
   * <p>This call is blocking. In case there is an asynchronous error, the handler must call {@link
   * #release} to signal the release of the grant and unblock this call.
   *
   * @return true if the counterpart has been unloaded, false if the current grant is expired or
   *     terminated.
   */
  boolean waitForOtherGrantToLoad();

  /**
   * Adds a callback to run when the grant is terminated. The handler must stop processing the grant
   * immediately after the callback is called.
   */
  void onTermination(Runnable r);

  /**
   * Release returns ownership of the grant immediately. Calling this method will cause the waitFor
   * methods to return false. It should be called when the handler is exiting, e.g. due to an
   * unexpected error.
   */
  void release();

  /**
   * @return cluster information with all consumers joined the work distribution process and their
   *     grants.
   */
  Cluster getCluster();
}
