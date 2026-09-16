package co.atoms.splitter.model;

/** DomainType represents the type of domain. */
public enum DomainType {
  INVALID,
  /**
   * Domain is an indivisible unit, useful for singletons, e.g. leader election. Key is not used.
   */
  UNIT,
  /** Domain is a global UUID range. */
  GLOBAL,
  /** Domain is a set of region-explicit UUID ranges. Each key must be qualified by a region. */
  REGIONAL;

  public static DomainType fromProto(co.atoms.splitter.proto.DomainType proto) {
    switch (proto) {
      case UNIT:
        return UNIT;
      case GLOBAL:
        return GLOBAL;
      case REGIONAL:
        return REGIONAL;
      default:
        return INVALID;
    }
  }

  public co.atoms.splitter.proto.DomainType toProto() {
    switch (this) {
      case UNIT:
        return co.atoms.splitter.proto.DomainType.UNIT;
      case GLOBAL:
        return co.atoms.splitter.proto.DomainType.GLOBAL;
      case REGIONAL:
        return co.atoms.splitter.proto.DomainType.REGIONAL;
      default:
        return co.atoms.splitter.proto.DomainType.INVALID;
    }
  }
}
