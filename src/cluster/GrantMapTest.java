package co.atoms.splitter.cluster;

import static co.atoms.splitter.testing.Fixtures.DOMAIN1;
import static co.atoms.splitter.testing.Fixtures.DOMAIN2;
import static co.atoms.splitter.testing.Fixtures.DOMAIN3;
import static co.atoms.splitter.testing.Fixtures.SHARD_D1_UNIT_0e;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_15;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_5c;
import static co.atoms.splitter.testing.Fixtures.SHARD_D3_REGIONAL_r1_1a;
import static co.atoms.splitter.testing.Fixtures.SHARD_D3_REGIONAL_r1_af;
import static co.atoms.splitter.testing.Fixtures.SHARD_D3_REGIONAL_r2_0a;
import static co.atoms.splitter.testing.Fixtures.SHARD_D3_REGIONAL_r2_af;
import static co.atoms.splitter.testing.Fixtures.pad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.model.DomainKey;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class GrantMapTest {

  @Test
  public void unitGrants() {
    GrantMap<Integer> m = new GrantMap<>();

    assertTrue(m.getDomains().isEmpty());
    assertEquals(m.getDomainGrants(DOMAIN1), List.of());

    m.write("g", SHARD_D1_UNIT_0e, GrantState.ALLOCATED, 42);

    var key = QualifiedDomainKey.create(DOMAIN1, new DomainKey(pad("2")));
    assertEquals(m.lookup(key), Optional.empty());

    int value = 43;
    for (var state : ClusterMap.LOOKUP_DEFAULT_GRANT_STATES) {
      m.write("g", SHARD_D1_UNIT_0e, state, value);
      assertEquals(m.lookup(key), Optional.of(value));
      value++;
    }

    m.loaded("g", SHARD_D1_UNIT_0e, value);
    assertEquals(m.lookup(key), Optional.of(value));
    value++;

    m.activate("g", SHARD_D1_UNIT_0e, value);
    assertEquals(m.lookup(key), Optional.of(value));
    value++;

    m.unloaded("g", SHARD_D1_UNIT_0e, value);
    assertEquals(m.lookup(key), Optional.of(value));
    value++;

    m.revoke("g", SHARD_D1_UNIT_0e, value);
    assertEquals(m.lookup(key), Optional.of(value));

    assertEquals(m.getDomainGrants(DOMAIN1), List.of(value));

    m.delete("g", SHARD_D1_UNIT_0e);

    assertEquals(m.getDomainGrants(DOMAIN1), List.of());
    assertEquals(m.getDomains(), List.of(DOMAIN1));
  }

  @Test
  public void globalGrants() {
    GrantMap<Integer> m = new GrantMap<>();

    assertTrue(m.getDomains().isEmpty());
    assertEquals(m.getDomainGrants(DOMAIN2), List.of());

    var key1 = QualifiedDomainKey.create(DOMAIN2, new DomainKey(pad("2")));
    var key2 = QualifiedDomainKey.create(DOMAIN2, new DomainKey(pad("6")));
    var value1 = 11;
    var value2 = 65;

    m.loaded("g1", SHARD_D2_GLOBAL_15, value1);
    m.loaded("g2", SHARD_D2_GLOBAL_5c, value2);
    assertEquals(m.lookup(key1), Optional.of(value1));
    assertEquals(m.lookup(key2), Optional.of(value2));

    value1++;
    m.activate("g1", SHARD_D2_GLOBAL_15, value1);
    assertEquals(m.lookup(key1), Optional.of(value1));
    assertEquals(m.lookup(key2), Optional.of(value2));

    assertEquals(
        m.getDomainGrants(DOMAIN2).stream().sorted().collect(Collectors.toList()),
        List.of(value1, value2));
    assertEquals(m.getDomains(), List.of(DOMAIN2));

    m.delete("g1", SHARD_D2_GLOBAL_15);

    assertEquals(
        m.getDomainGrants(DOMAIN2).stream().sorted().collect(Collectors.toList()), List.of(value2));
    assertEquals(m.getDomains(), List.of(DOMAIN2));
  }

  @Test
  public void regionalGrants() {
    GrantMap<Integer> m = new GrantMap<>();

    assertTrue(m.getDomains().isEmpty());
    assertEquals(m.getDomainGrants(DOMAIN3), List.of());

    var key1 = QualifiedDomainKey.create(DOMAIN3, new DomainKey("region1", pad("2")));
    var key2 = QualifiedDomainKey.create(DOMAIN3, new DomainKey("region1", pad("e")));
    var key3 = QualifiedDomainKey.create(DOMAIN3, new DomainKey("region2", pad("2")));
    var key4 = QualifiedDomainKey.create(DOMAIN3, new DomainKey("region2", pad("e")));
    var value1 = 11;
    var value2 = 65;
    var value3 = 33;
    var value4 = 78;

    m.loaded("g1", SHARD_D3_REGIONAL_r1_1a, value1);
    m.loaded("g2", SHARD_D3_REGIONAL_r1_af, value2);
    m.loaded("g3", SHARD_D3_REGIONAL_r2_0a, value3);
    m.loaded("g4", SHARD_D3_REGIONAL_r2_af, value4);
    assertEquals(m.lookup(key1), Optional.of(value1));
    assertEquals(m.lookup(key2), Optional.of(value2));
    assertEquals(m.lookup(key3), Optional.of(value3));
    assertEquals(m.lookup(key4), Optional.of(value4));

    value1++;
    value3++;
    m.activate("g1", SHARD_D3_REGIONAL_r1_1a, value1);
    m.activate("g3", SHARD_D3_REGIONAL_r2_0a, value3);
    assertEquals(m.lookup(key1), Optional.of(value1));
    assertEquals(m.lookup(key2), Optional.of(value2));
    assertEquals(m.lookup(key3), Optional.of(value3));
    assertEquals(m.lookup(key4), Optional.of(value4));

    assertEquals(
        m.getDomainGrants(DOMAIN3).stream().sorted().collect(Collectors.toList()),
        List.of(value1, value3, value2, value4));
    assertEquals(m.getDomains(), List.of(DOMAIN3));

    m.delete("g2", SHARD_D3_REGIONAL_r1_af);

    assertEquals(
        m.getDomainGrants(DOMAIN3).stream().sorted().collect(Collectors.toList()),
        List.of(value1, value3, value4));
    assertEquals(m.getDomains(), List.of(DOMAIN3));
  }

  @Test
  void multipleDomains() {
    GrantMap<Integer> m = new GrantMap<>();

    assertTrue(m.getDomains().isEmpty());
    assertEquals(m.getDomainGrants(DOMAIN3), List.of());

    m.write("g1", SHARD_D1_UNIT_0e, GrantState.ALLOCATED, 42);
    m.loaded("g2", SHARD_D2_GLOBAL_15, 43);
    m.revoke("g3", SHARD_D3_REGIONAL_r1_1a, 44);

    assertEquals(
        m.getDomains().stream()
            .sorted(Comparator.comparing(QualifiedDomainName::name))
            .collect(Collectors.toList()),
        List.of(DOMAIN1, DOMAIN2, DOMAIN3));
    assertEquals(m.getDomainGrants(DOMAIN1), List.of(42));
    assertEquals(m.getDomainGrants(DOMAIN2), List.of(43));
    assertEquals(m.getDomainGrants(DOMAIN3), List.of(44));
  }
}
