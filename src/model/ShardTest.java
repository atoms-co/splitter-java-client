package co.atoms.splitter.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class ShardTest {
  private static final QualifiedDomainName DOMAIN =
      QualifiedDomainName.parse("tenant/service/domain");
  private static final UUID FROM = UUID.fromString("10000000-0000-0000-0000-000000000000");
  private static final UUID TO = UUID.fromString("60000000-0000-0000-0000-000000000000");
  private static final Shard UNIT = Shard.create(DOMAIN, DomainType.UNIT, "", FROM, TO);
  private static final Shard GLOBAL = Shard.create(DOMAIN, DomainType.GLOBAL, "", FROM, TO);
  private static final Shard REGIONAL =
      Shard.create(DOMAIN, DomainType.REGIONAL, "region", FROM, TO);

  @Test
  public void toStringUnit() {
    assertEquals(UNIT.toString(), "tenant/service/domain");
  }

  @Test
  public void toStringGlobal() {
    assertEquals(GLOBAL.toString(), "tenant/service/domain[1000-6000)");
  }

  @Test
  public void toStringGlobalWithMin() {
    var shard =
        Shard.create(
            DOMAIN,
            DomainType.GLOBAL,
            "",
            UUID.fromString("00000000-0000-0000-0000-000000000000"),
            TO);
    assertEquals(shard.toString(), "tenant/service/domain[0000-6000)");
  }

  @Test
  public void toStringRegional() {
    assertEquals(REGIONAL.toString(), "tenant/service/domain@region[1000-6000)");
  }

  @Test
  public void containsInvalidDomain() {
    assertFalse(
        UNIT.contains(
            QualifiedDomainKey.create(
                QualifiedDomainName.parse("tenant/service/domain1"), new DomainKey(FROM))));
  }

  @Test
  public void containsUnit() {
    assertTrue(
        UNIT.contains(
            QualifiedDomainKey.create(
                DOMAIN, new DomainKey(UUID.fromString("f0000000-0000-0000-0000-000000000000")))));
  }

  @Test
  public void containsGlobal() {
    assertFalse(
        GLOBAL.contains(
            QualifiedDomainKey.create(
                DOMAIN, new DomainKey(UUID.fromString("00000000-0000-0000-0000-000000000000")))));
    assertTrue(GLOBAL.contains(QualifiedDomainKey.create(DOMAIN, new DomainKey(FROM))));
    assertTrue(
        GLOBAL.contains(
            QualifiedDomainKey.create(
                DOMAIN, new DomainKey(UUID.fromString("11000000-0000-0000-0000-000000000000")))));
    assertFalse(GLOBAL.contains(QualifiedDomainKey.create(DOMAIN, new DomainKey(TO))));
    assertFalse(
        GLOBAL.contains(
            QualifiedDomainKey.create(
                DOMAIN, new DomainKey(UUID.fromString("70000000-0000-0000-0000-000000000000")))));
  }

  @Test
  public void containsRegional() {
    assertFalse(
        REGIONAL.contains(QualifiedDomainKey.create(DOMAIN, new DomainKey("region1", FROM))));
    assertFalse(
        REGIONAL.contains(
            QualifiedDomainKey.create(
                DOMAIN,
                new DomainKey("region", UUID.fromString("00000000-0000-0000-0000-000000000000")))));
    assertTrue(REGIONAL.contains(QualifiedDomainKey.create(DOMAIN, new DomainKey("region", FROM))));
    assertTrue(
        REGIONAL.contains(
            QualifiedDomainKey.create(
                DOMAIN,
                new DomainKey("region", UUID.fromString("11000000-0000-0000-0000-000000000000")))));
    assertFalse(REGIONAL.contains(QualifiedDomainKey.create(DOMAIN, new DomainKey("region", TO))));
    assertFalse(
        REGIONAL.contains(
            QualifiedDomainKey.create(
                DOMAIN,
                new DomainKey("region", UUID.fromString("70000000-0000-0000-0000-000000000000")))));
  }
}
