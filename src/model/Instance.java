package co.atoms.splitter.model;

import co.atoms.splitter.utils.TimeUtils;
import java.time.Instant;

public record Instance(
    String id, Location location, Instant created, String name, String endpoint) {
  public static Instance create(
      String id, Location location, Instant created, String name, String endpoint) {
    return new Instance(id, location, created, name, endpoint);
  }

  public static Instance fromProto(co.atoms.splitter.proto.Instance instance) {
    var i = instance.getInstance();
    return create(
        i.getId(),
        Location.fromProto(i.getLocation()),
        TimeUtils.toInstant(i.getCreated()),
        i.getName(),
        instance.getEndpoint());
  }

  public co.atoms.splitter.proto.Instance toProto() {
    return co.atoms.splitter.proto.Instance.newBuilder()
        .setInstance(
            co.atoms.lib.net.location.proto.Instance.newBuilder()
                .setId(id)
                .setLocation(location.toProto())
                .setCreated(TimeUtils.toTimestamp(created))
                .setName(name)
                .build())
        .setEndpoint(endpoint)
        .build();
  }
}
