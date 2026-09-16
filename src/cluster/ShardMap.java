package co.atoms.splitter.cluster;

import co.atoms.splitter.model.DomainType;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.utils.BinarySearch;
import co.atoms.splitter.utils.UUIDComparator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ShardMap allows to store and retrieve shard-specific key-value pairs per domain.
 *
 * <p>Safe for concurrent access.
 */
public class ShardMap<K extends Comparable<K>, V> {

  /** ShardKV represents a shard-specific key-value pair. */
  public record ShardKV<K extends Comparable<K>, V>(Shard shard, K key, V value)
      implements Comparable<ShardKV<K, V>> {

    public static <K extends Comparable<K>, V> ShardKV<K, V> create(Shard shard, K key, V value) {
      return new ShardKV<>(shard, key, value);
    }

    public int compareTo(ShardKV<K, V> other) {
      return UUIDComparator.INSTANCE.compare(shard.from(), other.shard().from());
    }

    @Override
    public String toString() {
      return String.format("%s;%s=%s", shard(), key(), value());
    }
  }

  private final ConcurrentMap<QualifiedDomainName, DomainShardMap<K, V>> domains;

  public ShardMap() {
    domains = new ConcurrentHashMap<>();
  }

  /** Returns known domains in the map. */
  public List<QualifiedDomainName> getDomains() {
    return new ArrayList<>(domains.keySet());
  }

  /**
   * Returns all shard-specific key-value pairs for the given domain or empty list for an unknown
   * domain.
   */
  public List<ShardKV<K, V>> getShards(QualifiedDomainName domain) {
    if (!domains.containsKey(domain)) {
      return ImmutableList.of();
    }
    return domains.get(domain).getShards();
  }

  /** Returns the value for the given shard and key or empty if the shard or domain is unknown. */
  public Optional<V> read(Shard shard, K key) {
    if (!domains.containsKey(shard.domain())) {
      return Optional.empty();
    }
    return domains.get(shard.domain()).read(shard, key);
  }

  /**
   * Writes the value for the given shard and key. The value will be overwritten if the key already
   * exists.
   */
  public void write(Shard shard, K key, V value) {
    DomainShardMap<K, V> domainEntry =
        domains.computeIfAbsent(shard.domain(), d -> new DomainShardMap<>(shard.domainType()));
    domainEntry.write(shard, key, value);
  }

  /** Deletes the value for the given shard and key if exists, otherwise nothing happens. */
  public void delete(Shard shard, K key) {
    var domain = domains.get(shard.domain());
    if (domain == null) {
      return;
    }
    domain.delete(shard, key);
  }

  /** Lookup returns the shard-specific key-value pairs for the given key in the domain. */
  public List<ShardKV<K, V>> lookup(QualifiedDomainKey key) {
    var domain = domains.get(key.domain());
    if (domain == null) {
      return ImmutableList.of();
    }
    return domain.lookup(key);
  }

  public String toString() {
    return "ShardMap{domains=" + domains + "}";
  }
}

/** DomainShardMap manages shard-specific key-value pairs for a single domain of a known type. */
class DomainShardMap<K extends Comparable<K>, V> {

  private static final String DUMMY_REGION = "";

  private final DomainType domainType;

  private ImmutableMap<String, ImmutableList<ShardMap.ShardKV<K, V>>> lookupShards;
  private final Map<ShardKey<K>, V> shards = new HashMap<>();
  private boolean isInitialized = false;

  DomainShardMap(DomainType domainType) {
    this.domainType = domainType;
    this.lookupShards = ImmutableMap.of();
  }

  synchronized ImmutableList<ShardMap.ShardKV<K, V>> getShards() {
    return shards.entrySet().stream()
        .map(e -> e.getKey().toShardKV(e.getValue()))
        .collect(ImmutableList.toImmutableList());
  }

  synchronized Optional<V> read(Shard shard, K key) {
    return Optional.ofNullable(shards.get(new ShardKey<>(shard, key)));
  }

  synchronized void write(Shard shard, K key, V value) {
    shards.put(new ShardKey<>(shard, key), value);
    invalidate();
    isInitialized = false;
  }

  synchronized void delete(Shard shard, K key) {
    shards.remove(new ShardKey<>(shard, key));
    invalidate();
    isInitialized = false;
  }

  synchronized List<ShardMap.ShardKV<K, V>> lookup(QualifiedDomainKey key) {
    initIfNeeded();
    return switch (domainType) {
      case UNIT -> lookupShards.getOrDefault(DUMMY_REGION, ImmutableList.of());
      case GLOBAL ->
          findEnclosing(lookupShards.getOrDefault(DUMMY_REGION, ImmutableList.of()), key);
      case REGIONAL ->
          findEnclosing(lookupShards.getOrDefault(key.key().region(), ImmutableList.of()), key);
      default -> throw new IllegalArgumentException("Unknown domain type: " + domainType);
    };
  }

  private void invalidate() {
    lookupShards = ImmutableMap.of();
  }

  private void initIfNeeded() {
    if (!isInitialized) {
      switch (domainType) {
        case UNIT:
          lookupShards = ImmutableMap.of(DUMMY_REGION, getShards());
          break;
        case GLOBAL:
          lookupShards = ImmutableMap.of(DUMMY_REGION, getGlobalShards());
          break;
        case REGIONAL:
          lookupShards = getRegionalShards();
          break;
        default:
          throw new IllegalArgumentException("Unknown domain type: " + domainType);
      }
      isInitialized = true;
    }
  }

  private ImmutableList<ShardMap.ShardKV<K, V>> getGlobalShards() {
    // Sort for binary search
    return shards.entrySet().stream()
        .map(e -> e.getKey().toShardKV(e.getValue()))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  private ImmutableMap<String, ImmutableList<ShardMap.ShardKV<K, V>>> getRegionalShards() {
    Map<String, List<ShardMap.ShardKV<K, V>>> shards = new HashMap<>();
    for (var entry : this.shards.entrySet()) {
      ShardKey<K> shardKey = entry.getKey();
      ShardMap.ShardKV<K, V> shardKV = shardKey.toShardKV(entry.getValue());
      String region = shardKey.shard().region();
      if (!shards.containsKey(region)) {
        shards.put(region, new ArrayList<>());
      }
      shards.get(region).add(shardKV);
    }
    // Sort for binary search
    return shards.entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                Map.Entry::getKey,
                e -> e.getValue().stream().sorted().collect(ImmutableList.toImmutableList())));
  }

  private ImmutableList<ShardMap.ShardKV<K, V>> findEnclosing(
      ImmutableList<ShardMap.ShardKV<K, V>> shards, QualifiedDomainKey key) {
    var pos = BinarySearch.binarySearch(shards, shard -> compareKeyToShard(shard, key));
    if (pos < 0) {
      return ImmutableList.of();
    }
    // Find the first shard that contains the key
    while (pos > 0 && shards.get(pos - 1).shard().contains(key)) {
      pos--;
    }

    var builder = ImmutableList.<ShardMap.ShardKV<K, V>>builder();
    for (int i = pos; i < shards.size(); i++) {
      var shard = shards.get(i);
      if (shard.shard().contains(key)) {
        builder.add(shard);
      } else {
        break;
      }
    }
    return builder.build();
  }

  private int compareKeyToShard(ShardMap.ShardKV<K, V> shard, QualifiedDomainKey domainKey) {
    var key = domainKey.key().key();
    if (UUIDComparator.isBefore(key, shard.shard().from())) {
      // shard is larger then key
      return 1;
    }
    if (!UUIDComparator.isBefore(key, shard.shard().to())) {
      // shard is smaller then key
      return -1;
    }
    return 0;
  }

  public String toString() {
    return "DomainShardMap{domainType="
        + domainType
        + ", initialized="
        + isInitialized
        + ", shards="
        + shards
        + ", lookupShards="
        + lookupShards
        + "}";
  }

  record ShardKey<K extends Comparable<K>>(Shard shard, K key) {
    public <V> ShardMap.ShardKV<K, V> toShardKV(V value) {
      return ShardMap.ShardKV.create(shard, key, value);
    }
  }
}
