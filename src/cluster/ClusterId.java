package co.atoms.splitter.cluster;

import co.atoms.splitter.utils.TimeUtils;
import com.google.protobuf.TextFormat;
import java.time.Duration;
import java.time.Instant;

/**
 * Cluster identification information and metadata. Used for validation.
 *
 * @param origin Instance of the service component that originates the cluster map.
 * @param version Version of the cluster map. Version starts with 1 and is incremented for each
 *     update message
 * @param timestamp Cluster map creation timestamp.
 */
public record ClusterId(
    co.atoms.lib.net.location.proto.Instance origin, long version, Instant timestamp) {

  public static final ClusterId DEFAULT =
      create(co.atoms.lib.net.location.proto.Instance.getDefaultInstance(), 0, Instant.EPOCH);

  public static ClusterId create(
      co.atoms.lib.net.location.proto.Instance origin, long version, Instant timestamp) {
    return new ClusterId(origin, version, timestamp);
  }

  /** Creates a new cluster ID with the next version number. */
  public ClusterId next(Instant timestamp) {
    return create(origin, version + 1, timestamp);
  }

  /** Verifies that the given instance ID and version are the next in the sequence. */
  public boolean isNext(String instanceId, long version) {
    return origin.getId().equals(instanceId) && this.version + 1 == version;
  }

  @Override
  public String toString() {
    return String.format(
        "%d[origin=%s, updated=%s]",
        version,
        TextFormat.shortDebugString(origin),
        Duration.between(TimeUtils.toInstant(origin.getCreated()), timestamp));
  }
}
