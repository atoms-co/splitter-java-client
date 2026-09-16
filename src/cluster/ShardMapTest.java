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
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class ShardMapTest {

  @Test
  public void getDomains() {
    ShardMap<String, Integer> m = new ShardMap<>();
    assertTrue(m.getDomains().isEmpty());

    m.write(SHARD_D1_UNIT_0e, "key1", 11);
    assertEquals(m.getDomains(), List.of(DOMAIN1));

    m.write(SHARD_D2_GLOBAL_15, "key1", 12);
    m.write(SHARD_D3_REGIONAL_r1_1a, "key1", 45);
    assertEquals(
        m.getDomains().stream()
            .sorted(Comparator.comparing(QualifiedDomainName::toString))
            .collect(Collectors.toList()),
        List.of(DOMAIN1, DOMAIN2, DOMAIN3));

    // Domain is not removed when all shards are deleted
    m.delete(SHARD_D2_GLOBAL_15, "key1");
    assertEquals(
        m.getDomains().stream()
            .sorted(Comparator.comparing(QualifiedDomainName::toString))
            .collect(Collectors.toList()),
        List.of(DOMAIN1, DOMAIN2, DOMAIN3));
  }

  @Test
  public void getShards() {
    ShardMap<String, Integer> m = new ShardMap<>();
    assertTrue(m.getShards(DOMAIN1).isEmpty());

    m.write(SHARD_D1_UNIT_0e, "key1", 11);
    m.write(SHARD_D1_UNIT_0e, "key2", 23);
    var kv11 = ShardMap.ShardKV.create(SHARD_D1_UNIT_0e, "key1", 11);
    var kv12 = ShardMap.ShardKV.create(SHARD_D1_UNIT_0e, "key2", 23);
    assertEquals(sorted(m.getShards(DOMAIN1)), List.of(kv11, kv12));
    assertTrue(m.getShards(DOMAIN2).isEmpty());
    assertTrue(m.getShards(DOMAIN3).isEmpty());

    m.write(SHARD_D2_GLOBAL_15, "key1", 12);
    m.write(SHARD_D3_REGIONAL_r1_1a, "key1", 45);
    var kv21 = ShardMap.ShardKV.create(SHARD_D2_GLOBAL_15, "key1", 12);
    var kv31 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r1_1a, "key1", 45);
    assertEquals(sorted(m.getShards(DOMAIN1)), List.of(kv11, kv12));
    assertEquals(m.getShards(DOMAIN2), List.of(kv21));
    assertEquals(m.getShards(DOMAIN3), List.of(kv31));

    m.delete(SHARD_D1_UNIT_0e, "key1");
    m.delete(SHARD_D2_GLOBAL_15, "key1");
    assertEquals(m.getShards(DOMAIN1), List.of(kv12));
    assertTrue(m.getShards(DOMAIN2).isEmpty());
    assertEquals(m.getShards(DOMAIN3), List.of(kv31));
  }

  @Test
  void read() {
    ShardMap<String, Integer> m = new ShardMap<>();
    assertTrue(m.read(SHARD_D1_UNIT_0e, "key1").isEmpty());

    m.write(SHARD_D1_UNIT_0e, "key1", 11);
    assertEquals(m.read(SHARD_D1_UNIT_0e, "key1"), Optional.of(11));
    assertTrue(m.read(SHARD_D1_UNIT_0e, "key2").isEmpty());

    m.delete(SHARD_D1_UNIT_0e, "key1");
    assertTrue(m.read(SHARD_D1_UNIT_0e, "key1").isEmpty());

    m.write(SHARD_D2_GLOBAL_15, "key1", 12);
    assertTrue(m.read(SHARD_D1_UNIT_0e, "key1").isEmpty());
  }

  @Test
  void lookupUnit() {
    // Nothing to lookup in an empty map
    ShardMap<String, Integer> m = new ShardMap<>();
    var key = QualifiedDomainKey.create(DOMAIN1, new DomainKey(pad("f")));
    assertTrue(m.lookup(key).isEmpty());

    // Written key is found
    m.write(SHARD_D1_UNIT_0e, "key1", 11);
    var kv11 = ShardMap.ShardKV.create(SHARD_D1_UNIT_0e, "key1", 11);
    assertEquals(m.lookup(key), List.of(kv11));

    // Multiple keys are found
    m.write(SHARD_D1_UNIT_0e, "key2", 12);
    var kv12 = ShardMap.ShardKV.create(SHARD_D1_UNIT_0e, "key2", 12);
    assertEquals(sorted(m.lookup(key)), List.of(kv11, kv12));

    // Overwritten value is found
    m.write(SHARD_D1_UNIT_0e, "key1", 111);
    assertEquals(
        sorted(m.lookup(key)),
        List.of(ShardMap.ShardKV.create(SHARD_D1_UNIT_0e, "key1", 111), kv12));

    // Deleted key is not found
    m.delete(SHARD_D1_UNIT_0e, "key1");
    assertEquals(m.lookup(key), List.of(kv12));

    // Key with different domain is not found
    assertTrue(m.lookup(QualifiedDomainKey.create(DOMAIN2, new DomainKey(pad("f")))).isEmpty());

    // Key is found after adding a new pair with the same key and a different domain
    m.write(SHARD_D2_GLOBAL_15, "key1", 23);
    assertEquals(m.lookup(key), List.of(kv12));
  }

  @Test
  public void lookupGlobal() {
    ShardMap<String, Integer> m = new ShardMap<>();
    assertTrue(m.lookup(key(DOMAIN2, "2")).isEmpty());

    // Write multiple shards and values, and verify
    m.write(SHARD_D2_GLOBAL_15, "key1", 21);
    m.write(SHARD_D2_GLOBAL_15, "key2", 22);
    m.write(SHARD_D2_GLOBAL_5c, "key3", 23);

    var kv1 = ShardMap.ShardKV.create(SHARD_D2_GLOBAL_15, "key1", 21);
    var kv2 = ShardMap.ShardKV.create(SHARD_D2_GLOBAL_15, "key2", 22);
    var kv3 = ShardMap.ShardKV.create(SHARD_D2_GLOBAL_5c, "key3", 23);

    assertEquals(m.lookup(key(DOMAIN2, "0")), List.of());
    assertEquals(sorted(m.lookup(key(DOMAIN2, "1"))), List.of(kv1, kv2));
    assertEquals(sorted(m.lookup(key(DOMAIN2, "6"))), List.of(kv3));
    assertEquals(m.lookup(key(DOMAIN2, "d")), List.of());

    // Delete unknown key, verify no changes
    m.delete(SHARD_D2_GLOBAL_15, "key3");
    assertEquals(m.lookup(key(DOMAIN2, "0")), List.of());
    assertEquals(sorted(m.lookup(key(DOMAIN2, "1"))), List.of(kv1, kv2));
    assertEquals(sorted(m.lookup(key(DOMAIN2, "6"))), List.of(kv3));
    assertEquals(m.lookup(key(DOMAIN2, "d")), List.of());

    // Delete existing key and verify changes
    m.delete(SHARD_D2_GLOBAL_15, "key2");
    assertEquals(m.lookup(key(DOMAIN2, "0")), List.of());
    assertEquals(sorted(m.lookup(key(DOMAIN2, "1"))), List.of(kv1));
    assertEquals(sorted(m.lookup(key(DOMAIN2, "6"))), List.of(kv3));
    assertEquals(m.lookup(key(DOMAIN2, "d")), List.of());

    // Overwritten value is found
    m.write(SHARD_D2_GLOBAL_15, "key1", 211);
    assertEquals(m.lookup(key(DOMAIN2, "0")), List.of());
    assertEquals(
        sorted(m.lookup(key(DOMAIN2, "1"))),
        List.of(ShardMap.ShardKV.create(SHARD_D2_GLOBAL_15, "key1", 211)));
    assertEquals(sorted(m.lookup(key(DOMAIN2, "6"))), List.of(kv3));
    assertEquals(m.lookup(key(DOMAIN2, "d")), List.of());

    // Key with different domain is not found
    assertTrue(m.lookup(key(DOMAIN1, "1")).isEmpty());

    // Key is found after adding a new pair with the same key and a different domain
    m.write(SHARD_D1_UNIT_0e, "key3", 13);
    assertEquals(sorted(m.lookup(key(DOMAIN2, "6"))), List.of(kv3));
  }

  @Test
  public void lookupRegional() {
    ShardMap<String, Integer> m = new ShardMap<>();
    assertTrue(m.lookup(key(DOMAIN3, "region1", "2")).isEmpty());

    // Write multiple regional shards and values, and verify
    m.write(SHARD_D3_REGIONAL_r1_1a, "key1", 31);
    m.write(SHARD_D3_REGIONAL_r1_af, "key2", 32);
    m.write(SHARD_D3_REGIONAL_r1_af, "key3", 33);
    m.write(SHARD_D3_REGIONAL_r2_0a, "key1", 34);
    m.write(SHARD_D3_REGIONAL_r2_af, "key2", 35);

    var kv11 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r1_1a, "key1", 31);
    var kv12 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r1_af, "key2", 32);
    var kv13 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r1_af, "key3", 33);
    var kv21 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r2_0a, "key1", 34);
    var kv22 = ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r2_af, "key2", 35);

    // region r1
    assertEquals(m.lookup(key(DOMAIN3, "region1", "0")), List.of());
    assertEquals(m.lookup(key(DOMAIN3, "region1", "1")), List.of(kv11));
    assertEquals(sorted(m.lookup(key(DOMAIN3, "region1", "b"))), List.of(kv12, kv13));
    assertEquals(m.lookup(key(DOMAIN3, "region1", "ff")), List.of());
    // region r2
    assertEquals(m.lookup(key(DOMAIN3, "region2", "0")), List.of(kv21));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "b")), List.of(kv22));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "ff")), List.of());

    // Delete unknown key, verify no changes
    m.delete(SHARD_D3_REGIONAL_r1_af, "key4");
    assertEquals(m.lookup(key(DOMAIN3, "region1", "0")), List.of());
    assertEquals(m.lookup(key(DOMAIN3, "region1", "1")), List.of(kv11));
    assertEquals(sorted(m.lookup(key(DOMAIN3, "region1", "b"))), List.of(kv12, kv13));
    assertEquals(m.lookup(key(DOMAIN3, "region1", "ff")), List.of());
    assertEquals(m.lookup(key(DOMAIN3, "region2", "0")), List.of(kv21));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "b")), List.of(kv22));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "ff")), List.of());

    // Delete existing key and verify changes
    m.delete(SHARD_D3_REGIONAL_r1_af, "key3");
    assertEquals(m.lookup(key(DOMAIN3, "region1", "0")), List.of());
    assertEquals(m.lookup(key(DOMAIN3, "region1", "1")), List.of(kv11));
    assertEquals(m.lookup(key(DOMAIN3, "region1", "b")), List.of(kv12));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "0")), List.of(kv21));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "b")), List.of(kv22));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "ff")), List.of());

    // Overwritten value is found
    m.write(SHARD_D3_REGIONAL_r2_0a, "key1", 344);
    assertEquals(m.lookup(key(DOMAIN3, "region1", "0")), List.of());
    assertEquals(m.lookup(key(DOMAIN3, "region1", "1")), List.of(kv11));
    assertEquals(m.lookup(key(DOMAIN3, "region1", "b")), List.of(kv12));
    assertEquals(
        m.lookup(key(DOMAIN3, "region2", "0")),
        List.of(ShardMap.ShardKV.create(SHARD_D3_REGIONAL_r2_0a, "key1", 344)));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "b")), List.of(kv22));
    assertEquals(m.lookup(key(DOMAIN3, "region2", "ff")), List.of());

    // Key with different domain is not found
    assertTrue(m.lookup(key(DOMAIN1, "1")).isEmpty());

    // Key is found after adding a new pair with the same key and a different domain
    m.write(SHARD_D1_UNIT_0e, "key2", 13);
    assertEquals(m.lookup(key(DOMAIN3, "region2", "b")), List.of(kv22));
  }

  private static <K extends Comparable<K>, V> List<ShardMap.ShardKV<K, V>> sorted(
      List<ShardMap.ShardKV<K, V>> list) {
    return list.stream()
        .sorted(Comparator.comparing(ShardMap.ShardKV::key))
        .collect(Collectors.toList());
  }

  private static QualifiedDomainKey key(QualifiedDomainName domain, String prefix) {
    return QualifiedDomainKey.create(domain, new DomainKey(pad(prefix)));
  }

  private static QualifiedDomainKey key(QualifiedDomainName domain, String region, String prefix) {
    return QualifiedDomainKey.create(domain, new DomainKey(region, pad(prefix)));
  }
}
