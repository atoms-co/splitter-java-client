package co.atoms.splitter.model;

import java.util.UUID;

/** DomainKey specifies a domain element for an implicit domain. */
public record DomainKey(String region, UUID key) {
  public DomainKey(UUID key) {
    this("", key);
  }

  public static DomainKey fromProto(co.atoms.splitter.proto.DomainKey key) {
    return new DomainKey(key.getRegion(), UUID.fromString(key.getKey()));
  }

  public co.atoms.splitter.proto.DomainKey toProto() {
    return co.atoms.splitter.proto.DomainKey.newBuilder()
        .setRegion(region)
        .setKey(key.toString())
        .build();
  }
}
