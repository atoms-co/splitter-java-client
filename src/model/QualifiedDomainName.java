package co.atoms.splitter.model;

public record QualifiedDomainName(QualifiedServiceName service, String name) {

  public static QualifiedDomainName parse(String name) {
    String[] parts = name.split("/");
    if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
      throw new IllegalArgumentException("Invalid qualified domain name: " + name);
    }
    return create(QualifiedServiceName.create(parts[0], parts[1]), parts[2]);
  }

  public static QualifiedDomainName create(QualifiedServiceName service, String domain) {
    return new QualifiedDomainName(service, domain);
  }

  public static QualifiedDomainName fromProto(co.atoms.splitter.proto.QualifiedDomainName proto) {
    return create(QualifiedServiceName.fromProto(proto.getService()), proto.getName());
  }

  public co.atoms.splitter.proto.QualifiedDomainName toProto() {
    return co.atoms.splitter.proto.QualifiedDomainName.newBuilder()
        .setService(service.toProto())
        .setName(name)
        .build();
  }

  @Override
  public String toString() {
    return String.format("%s/%s", service, name);
  }
}
