package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.model.Shard;
import java.time.Duration;

/**
 * Range is responsible for handling of a range of keys. It has lifecycle methods to load, activate,
 * drain and terminate.
 */
public interface Range {

  @FunctionalInterface
  interface Factory<T extends Range> {
    /**
     * Factory method to create a new range.
     *
     * @param id unique id of the grant
     * @param shard the shard of the range
     * @param ownership the ownership of the range. Ownership must be used to signal abnormal
     *     termination of the range.
     */
    T create(String id, Shard shard, Ownership ownership);
  }

  /**
   * Initializes the range synchronously. Called after the range is created and before it is
   * activated. The range can be allocated (with another range in revoked state) or active (no other
   * owners of this range). See {@link co.atoms.splitter.model.GrantState} for more information.
   */
  void initialize();

  /**
   * Activates the range. This signal indicates that this range is a sole owner of the keys in the
   * range. This method should be non-blocking and return immediately.
   */
  void activateAsync();

  /**
   * Drains the range synchronously. This method is called when the grant of the range is revoked.
   * It indicates that the ownership of this range will move to a new owner in near future. The
   * range should release resources and prepare for termination.
   *
   * @param timeout expiration time of grant
   */
  void drain(Duration timeout);

  /**
   * Signals the range to terminate abruptly. The range should stop processing immediately and
   * release the resources. This method can be called concurrently with other methods.
   */
  void terminateAsync();
}
