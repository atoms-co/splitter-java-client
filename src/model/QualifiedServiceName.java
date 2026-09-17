package co.atoms.splitter.model;

public record QualifiedServiceName(String tenant, String name) {
  public static QualifiedServiceName parse(String name) {
    String[] parts = name.split("/");
    if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
      throw new IllegalArgumentException("Invalid qualified service name: " + name);
    }
    return create(parts[0], parts[1]);
  }

  public static QualifiedServiceName create(String tenant, String service) {
    return new QualifiedServiceName(tenant, service);
  }

  public static QualifiedServiceName fromProto(co.atoms.splitter.proto.QualifiedServiceName proto) {
    return new QualifiedServiceName(proto.getTenant(), proto.getService());
  }

  public co.atoms.splitter.proto.QualifiedServiceName toProto() {
    return co.atoms.splitter.proto.QualifiedServiceName.newBuilder()
        .setTenant(tenant())
        .setService(name())
        .build();
  }

  public String toString() {
    return String.format("%s/%s", tenant(), name());
  }
}
