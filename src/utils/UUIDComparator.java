package co.atoms.splitter.utils;

import java.util.Comparator;
import java.util.UUID;

/**
 * Comparator for UUID objects that provides RFC4122 sort order.
 *
 * <p>A workaround for the known JDK bug with Java's UUID sort order <a
 * href="https://bugs.openjdk.org/browse/JDK-7025832">JDK-7025832</a>.
 */
public class UUIDComparator implements Comparator<UUID> {

  public static final UUIDComparator INSTANCE = new UUIDComparator();

  @Override
  public int compare(UUID u1, UUID u2) {
    int diff = Long.compareUnsigned(u1.getMostSignificantBits(), u2.getMostSignificantBits());
    if (diff != 0) {
      return diff;
    }
    return Long.compareUnsigned(u1.getLeastSignificantBits(), u2.getLeastSignificantBits());
  }

  public static boolean isBefore(UUID u1, UUID u2) {
    return INSTANCE.compare(u1, u2) < 0;
  }

  public static boolean isAfter(UUID u1, UUID u2) {
    return INSTANCE.compare(u1, u2) > 0;
  }

  /** Returns true if the key is in the range [from,to). */
  public static boolean isBetween(UUID key, UUID from, UUID to) {
    if (isBefore(to, from)) {
      throw new IllegalArgumentException("to must be after from");
    }
    return !isBefore(key, from) && isBefore(key, to);
  }
}
