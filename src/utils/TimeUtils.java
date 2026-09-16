package co.atoms.splitter.utils;

import com.google.protobuf.Timestamp;
import java.time.Instant;

public class TimeUtils {
  private TimeUtils() {}

  public static Instant toInstant(final Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static Timestamp toTimestamp(final Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }
}
