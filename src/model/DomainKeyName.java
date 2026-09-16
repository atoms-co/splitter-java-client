package co.atoms.splitter.model;

/**
 * DomainKeyName is a name of a key in a domain (in service context).
 *
 * <p>A key is mapped to a name in the service configuration. Currently used for canary integration.
 */
public record DomainKeyName(String domain, String name) {

  public co.atoms.splitter.proto.DomainKeyName toProto() {
    return co.atoms.splitter.proto.DomainKeyName.newBuilder()
        .setDomain(domain)
        .setName(name)
        .build();
  }
}
