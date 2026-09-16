package co.atoms.splitter.model;

import java.util.UUID;

/** QualifiedDomainKey fully specifies a domain element. */
public record QualifiedDomainKey(QualifiedDomainName domain, DomainKey key) {
  public static QualifiedDomainKey create(QualifiedDomainName domain, UUID key) {
    return new QualifiedDomainKey(domain, new DomainKey(key));
  }

  public static QualifiedDomainKey create(QualifiedDomainName domain, String region, UUID key) {
    return new QualifiedDomainKey(domain, new DomainKey(region, key));
  }

  public static QualifiedDomainKey create(QualifiedDomainName domain, DomainKey key) {
    return new QualifiedDomainKey(domain, key);
  }

  public static QualifiedDomainKey fromProto(co.atoms.splitter.proto.QualifiedDomainKey key) {
    return create(
        QualifiedDomainName.fromProto(key.getDomain()), DomainKey.fromProto(key.getKey()));
  }

  public co.atoms.splitter.proto.QualifiedDomainKey toProto() {
    return co.atoms.splitter.proto.QualifiedDomainKey.newBuilder()
        .setDomain(domain().toProto())
        .setKey(key().toProto())
        .build();
  }
}
