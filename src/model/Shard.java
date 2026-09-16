package co.atoms.splitter.model;

import co.atoms.splitter.utils.UUIDComparator;
import java.util.UUID;

/**
 * Shard is a unit of work. Usually it is a continuous subrange of UUIDs, but can also span the
 * whole UUID space. The range of UUIDs is [from, to), it does not include the to value. Note that
 * with this definition, the last shard in UUID space will not include the last UUID value.
 *
 * @param domain The domain of the keys in the shard. Required.
 * @param domainType The type of the domain. Part of domain metadata included in a shard to avoid
 *     loading domain metadata from state.
 * @param region The region of the keys in the shard. Required for REGIONAL domain type.
 * @param from The start of the range of keys in the shard. Inclusive.
 * @param to The end of the range of keys in the shard. Exclusive.
 */
public record Shard(
    QualifiedDomainName domain, DomainType domainType, String region, UUID from, UUID to) {

  public static Shard create(
      QualifiedDomainName domain, DomainType domainType, String region, UUID from, UUID to) {
    return new Shard(domain, domainType, region, from, to);
  }

  public static Shard fromProto(co.atoms.splitter.proto.Shard shard) {
    return create(
        QualifiedDomainName.fromProto(shard.getDomain()),
        DomainType.fromProto(shard.getType()),
        shard.getRegion(),
        UUID.fromString(shard.getFrom()),
        UUID.fromString(shard.getTo()));
  }

  public co.atoms.splitter.proto.Shard toProto() {
    return co.atoms.splitter.proto.Shard.newBuilder()
        .setDomain(domain.toProto())
        .setType(domainType.toProto())
        .setRegion(region)
        .setFrom(from.toString())
        .setTo(to.toString())
        .build();
  }

  /**
   * Checks if the shard contains the given key. A shard contains a key if the key is in the same
   * domain, the region matches (if the shard is REGIONAL), and the key is in the range [from, to).
   *
   * @return true if the key is in the shard, false otherwise
   */
  public boolean contains(QualifiedDomainKey key) {
    if (!key.domain().equals(domain)) {
      return false;
    }
    return switch (domainType) {
      case UNIT -> true;
      case GLOBAL -> UUIDComparator.isBetween(key.key().key(), from(), to());
      case REGIONAL -> {
        if (!key.key().region().equals(region)) {
          yield false;
        }
        yield UUIDComparator.isBetween(key.key().key(), from(), to());
      }
      default -> false;
    };
  }

  @Override
  public String toString() {
    return switch (domainType) {
      case UNIT -> domain.toString();
      case GLOBAL -> String.format("%s[%s-%s)", domain, first(from(), 4), first(to(), 4));
      case REGIONAL ->
          String.format("%s@%s[%s-%s)", domain, region, first(from(), 4), first(to(), 4));
      default -> "invalid-shard";
    };
  }

  private static String first(UUID uuid, int n) {
    var s = uuid.toString();
    return s.substring(0, Math.min(s.length(), n));
  }
}
