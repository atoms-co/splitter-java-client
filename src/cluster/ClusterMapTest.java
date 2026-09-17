package co.atoms.splitter.cluster;

import static co.atoms.splitter.testing.Asserts.assertProtosEqual;
import static co.atoms.splitter.testing.Fixtures.newKey;
import static co.atoms.splitter.testing.Fixtures.newLocationInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.model.DomainType;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.testing.Fixtures;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.runfiles.Runfiles;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class ClusterMapTest {

  private static final ClusterId CLUSTER_ID =
      ClusterId.create(newLocationInstance("id1"), 1, Instant.now());
  private static final co.atoms.lib.net.location.proto.Instance newInstance = newLocationInstance("id");

  private static final String DEFAULT_DOMAIN = "t/s/d";
  private static final String TEST_CASES_DIR = "testcases/cluster";
  private static final Path TEST_CASES_ROOT = getTestcaseRoot();

  private static Path getTestcaseRoot() {
    var location = System.getenv("TESTCASES_LOCATIONS").split(" ")[0];
    var path = location.substring(3, location.indexOf(TEST_CASES_DIR) + TEST_CASES_DIR.length());
    try {
      return Paths.get(Runfiles.create().rlocation(Paths.get("").resolve(path).toString()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest(name = "{0}")
  @ArgumentsSource(TestCaseProvider.class)
  public void testClusterUpdateFromTestCase(Path testCaseFile) throws Exception {
    Yaml yaml = new Yaml(new Constructor(Actions.class, new LoaderOptions()));
    Actions actions =
        yaml.load(new FileInputStream(TEST_CASES_ROOT.resolve(testCaseFile).toFile()));

    var c = new ClusterMap(CLUSTER_ID, List.of());
    for (var a : actions.actions) {
      c = a.execute(c);
    }
  }

  public static class TestCaseProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      List<Path> files;
      try (Stream<Path> paths = Files.walk(TEST_CASES_ROOT)) {
        files =
            paths
                .filter(Files::isRegularFile)
                .map(
                    f ->
                        TEST_CASES_ROOT
                            .toAbsolutePath()
                            .relativize(f.toAbsolutePath()))
                .toList();
      }

      assertFalse(files.isEmpty(), "no test cases found");
      return files.stream().map(Arguments::of);
    }
  }

  public static class Actions {
    public List<Action> actions;
  }

  public static class Action {
    private static final List<ShardInfo> NO_SHARDS = new ArrayList<>();

    public String action;
    public long version = -1;
    public List<ShardInfo> shards = NO_SHARDS;
    public List<Consumer> assignments = List.of();
    public List<Lookup> lookups = List.of();
    public List<Grant> updated = List.of();
    public List<String> unassigned = List.of();
    public List<String> removed = List.of();
    public String expected_error;

    public ClusterMap execute(ClusterMap c) {
      System.out.printf("Executing action %s%n", action);
      return switch (action) {
        case "create" -> create();
        case "snapshot" -> snapshot(c);
        case "change" -> change(c);
        case "compare" -> {
          compare(c);
          yield c;
        }
        case "lookup" -> {
          lookup(c);
          yield c;
        }
        default -> throw new IllegalArgumentException("unknown action: %s".formatted(action));
      };
    }

    ClusterMap create() {
      return new ClusterMap(CLUSTER_ID, shards.stream().map(s -> parseShard(s.shard)).toList());
    }

    ClusterMap snapshot(ClusterMap c) {
      var shards =
          this.shards.stream()
              .map(s -> parseShard(s.shard))
              .collect(ImmutableList.toImmutableList());
      var snapshot =
          new Messages.Snapshot(
              assignments.stream().map(Consumer::toAssignment).toList(),
              c.getId().origin(),
              shards);
      var msg =
          new Messages.ClusterMessage(
              Optional.of(snapshot),
              Optional.empty(),
              c.getId().origin().getId(),
              version == -1 ? c.getId().version() + 1 : version,
              c.getId().timestamp());
      return update(c, msg);
    }

    ClusterMap change(ClusterMap c) {
      var assign = new Messages.Assign(assignments.stream().map(Consumer::toAssignment).toList());
      var update = new Messages.Update(updated.stream().map(Grant::toGrantInfo).toList());
      var unassign = new Messages.Unassign(unassigned);
      var remove = new Messages.Remove(removed);
      var shards =
          this.shards == NO_SHARDS
              ? Optional.<Messages.Shards>empty()
              : Optional.of(
                  new Messages.Shards(
                      this.shards.stream()
                          .map(s -> parseShard(s.shard))
                          .collect(ImmutableList.toImmutableList())));
      var change =
          new Messages.Change(
              Optional.of(assign),
              Optional.of(update),
              Optional.of(unassign),
              Optional.of(remove),
              shards);
      var msg =
          new Messages.ClusterMessage(
              Optional.empty(),
              Optional.of(change),
              c.getId().origin().getId(),
              version == -1 ? c.getId().version() + 1 : version,
              c.getId().timestamp());
      return update(c, msg);
    }

    private ClusterMap update(ClusterMap c, Messages.ClusterMessage msg) {
      try {
        c = c.update(msg);
      } catch (Exception e) {
        if (expected_error != null) {
          assertEquals(
              e.getMessage().toLowerCase(), expected_error.toLowerCase(), "unexpected error");
          return c;
        } else {
          throw e;
        }
      }
      if (expected_error != null) {
        throw new IllegalStateException(
            "expected error not detected: %s".formatted(expected_error));
      }
      return c;
    }

    void compare(ClusterMap c) {
      assertEquals(c.getId().version(), version);
      assertProtosEqual(c.getId().origin(), CLUSTER_ID.origin());

      // Compare shard grants
      Set<Shard> expectedShards = new HashSet<>();
      Map<QualifiedDomainName, List<Shard>> domains = new HashMap<>();

      for (var shard : shards) {
        var expectedShard = parseShard(shard.shard);
        expectedShards.add(expectedShard);
        domains.computeIfAbsent(expectedShard.domain(), k -> new ArrayList<>()).add(expectedShard);

        var actualGrants = c.getShardGrants(expectedShard);
        assertTrue(actualGrants.isPresent(), "shard not found: %s".formatted(expectedShard));

        var actual = new HashSet<>(actualGrants.get());
        var expected = new HashSet<>(shard.grants);
        assertEquals(actual, expected, "grants mismatch for shard %s".formatted(expectedShard));
      }
      // Compare shards
      assertEquals(new HashSet<>(c.getShards()), expectedShards, "total shards mismatch");

      // Check domain shards
      for (var e : domains.entrySet()) {
        var domain = e.getKey();
        var expected = new HashSet<>(e.getValue());
        var actual = new HashSet<>(c.getDomainShards(domain));
        assertEquals(actual, expected, "shards mismatch for domain %s".formatted(domain));
      }

      // Compare consumers and their assignments
      Set<Instance> expectedConsumers = new TreeSet<>(Comparator.comparing(Instance::id));
      Set<Messages.Assignment> expectedAssignments =
          new TreeSet<>(Comparator.comparing(a -> a.consumer().id()));
      for (var consumer : assignments) {
        var expectedConsumer = consumer.toAssignment().consumer();
        expectedConsumers.add(expectedConsumer);

        var actualConsumer = c.getConsumer(consumer.consumer);
        assertFalse(
            actualConsumer.isEmpty(), "consumer not found: %s".formatted(consumer.consumer));
        assertConsumersEqual(
            actualConsumer.get(),
            expectedConsumer,
            "consumer mismatch: %s".formatted(consumer.consumer));

        assertEquals(
            c.getConsumerRetainedVersion(consumer.consumer).orElse(0L),
            consumer.origin_cluster_version,
            "version mismatch for consumer %s".formatted(consumer.consumer));

        var cg = c.getConsumerGrants(consumer.consumer);
        assertFalse(cg.isEmpty(), "consumer not found: %s".formatted(consumer.consumer));
        var consumerGrants = cg.get();
        assertConsumersEqual(
            consumerGrants.consumer(),
            expectedConsumer,
            "consumer mismatch: %s".formatted(consumer.consumer));
        assertEquals(
            consumerGrants.grants().size(),
            consumer.grants.size(),
            "total grants mismatch for consumer %s".formatted(consumer.consumer));

        var actualGrants =
            consumerGrants.grants().stream().sorted(Comparator.comparing(GrantInfo::id)).toList();

        var expectedGrants = new TreeSet<>(Comparator.comparing(GrantInfo::id));
        for (var i = 0; i < consumer.grants.size(); ++i) {
          var grant = consumer.grants.get(i);
          var expectedGrant = grant.toGrantInfo();
          expectedGrants.add(expectedGrant);
          assertGrantsEqual(
              actualGrants.get(i), expectedGrant, "grant mismatch: %s".formatted(grant.id));

          var g = c.getGrant(grant.id);
          assertFalse(g.isEmpty(), "grant not found: %s".formatted(grant.id));
          assertGrantsEqual(
              g.get().grant(), expectedGrant, "grant mismatch: %s".formatted(grant.id));
          assertConsumersEqual(
              g.get().consumer(),
              expectedConsumer,
              "consumer mismatch for grant %s: %s".formatted(grant.id, consumer.consumer));

          var v = c.getGrantRetainedVersion(grant.id);
          assertEquals(
              v.orElse(0L),
              grant.origin_cluster_version,
              "version mismatch for grant %s".formatted(grant.id));
        }

        expectedAssignments.add(
            Messages.Assignment.create(expectedConsumer, expectedGrants.stream().toList()));
      }

      // Compare consumers
      var actualConsumers =
          c.getConsumers().stream().sorted(Comparator.comparing(Instance::id)).toList();
      assertEquals(actualConsumers.size(), expectedConsumers.size(), "total consumers mismatch");
      var expectedConsumersList = new ArrayList<>(expectedConsumers);
      for (var i = 0; i < actualConsumers.size(); ++i) {
        assertConsumersEqual(
            actualConsumers.get(i),
            expectedConsumersList.get(i),
            "consumer mismatch: %s".formatted(actualConsumers.get(i).id()));
      }

      // Compare assignments
      var actualAssignments =
          c.getAssignments().stream().sorted(Comparator.comparing(a -> a.consumer().id())).toList();
      assertEquals(
          actualAssignments.size(), expectedAssignments.size(), "total assignments mismatch");
      var expectedAssignmentsList = new ArrayList<>(expectedAssignments);
      for (int i = 0; i < actualAssignments.size(); ++i) {
        var actual = actualAssignments.get(i);
        var expected = expectedAssignmentsList.get(i);

        assertConsumersEqual(
            actual.consumer(),
            expected.consumer(),
            "consumer mismatch for assignment: %s".formatted(actual.consumer().id()));

        var grants = actual.grants().stream().sorted(Comparator.comparing(GrantInfo::id)).toList();

        assertEquals(
            grants.size(),
            expected.grants().size(),
            "total grants mismatch for consumer %s".formatted(actual.consumer().id()));
        for (int j = 0; j < grants.size(); ++j) {
          assertGrantsEqual(
              grants.get(j),
              expected.grants().get(j),
              "grant mismatch for consumer %s".formatted(actual.consumer().id()));
        }
      }
    }

    void lookup(ClusterMap c) {
      for (var lookup : lookups) {
        lookup.lookup(c);
      }
    }
  }

  public static class ShardInfo {
    public String shard;
    public List<String> grants = List.of();
  }

  private static Shard parseShard(String shard) {
    var domain = DEFAULT_DOMAIN;
    var dt = DomainType.GLOBAL;
    var shardRange = "";
    var region = "";
    for (var part : shard.split(",")) {
      var props = part.split("=");
      assertEquals(props.length, 2, "invalid shard property: %s".formatted(part));

      var key = props[0].trim();
      var value = props[1].trim();
      switch (key) {
        case "domain":
          domain = value;
          break;
        case "type":
          dt =
              switch (value) {
                case "G" -> DomainType.GLOBAL;
                case "R" -> DomainType.REGIONAL;
                case "U" -> DomainType.UNIT;
                default ->
                    throw new IllegalArgumentException("unknown domain type: %s".formatted(value));
              };
          break;
        case "range":
          shardRange = value;
          break;
        case "region":
          region = value;
          break;
        default:
          throw new IllegalArgumentException("unknown shard property: %s".formatted(key));
      }
    }

    assertEquals(shardRange.charAt(0), '(', "range must be in format (from:to)");
    assertEquals(
        shardRange.charAt(shardRange.length() - 1), ')', "range must be in format (from:to)");

    var rangeParts = shardRange.substring(1, shardRange.length() - 1).split(":");
    assertEquals(rangeParts.length, 2, "range must be in format (from:to)");

    var qdn = QualifiedDomainName.parse(domain);
    var from = Fixtures.pad(rangeParts[0]);
    var to = Fixtures.pad(rangeParts[1]);

    return Shard.create(qdn, dt, region, from, to);
  }

  public static class Consumer {
    public String consumer;
    public List<Grant> grants = List.of();
    public int origin_cluster_version;

    public Messages.Assignment toAssignment() {
      var c =
          Instance.create(
              consumer,
              Location.create("centralus", "node-%s".formatted(consumer)),
              Fixtures.TS,
              consumer,
              "50005");
      return Messages.Assignment.create(c, grants.stream().map(Grant::toGrantInfo).toList());
    }
  }

  public static class Grant {
    public String id;
    public String shard;
    public String state;
    public int origin_cluster_version;

    public GrantInfo toGrantInfo() {
      return GrantInfo.create(id, parseShard(shard), parseState(state));
    }
  }

  public static class Lookup {
    public String key;
    public String region;
    public String domain;
    public String states;
    public LookupResult expected;

    public void lookup(ClusterMap c) {
      System.out.printf(
          "Looking up {key: %s, states: %s, domain: %s, region: %s}%n",
          key, states, domain, region);

      var result = c.lookup(parseKey(), parseStates().toArray(new GrantState[0]));

      if (expected.found) {
        assertFalse(
            result.isEmpty(), "lookup failed for key %s and states %s".formatted(key, states));
        assertEquals(
            expected.consumer,
            result.get().consumer().id(),
            "unexpected consumer for key %s and states %s".formatted(key, states));
        assertEquals(
            expected.grant,
            result.get().grant().id(),
            "unexpected grant for key %s and states %s".formatted(key, states));
      } else {
        assertTrue(
            result.isEmpty(),
            "unexpected lookup success for key %s and states %s".formatted(key, states));
      }
    }

    QualifiedDomainKey parseKey() {
      if (domain == null || domain.isEmpty()) {
        domain = DEFAULT_DOMAIN;
      }
      return newKey(QualifiedDomainName.parse(domain), region, key);
    }

    List<GrantState> parseStates() {
      if (states == null || states.isEmpty()) {
        return List.of();
      }
      return Arrays.stream(states.split(","))
          .map(String::trim)
          .map(ClusterMapTest::parseState)
          .toList();
    }
  }

  public static class LookupResult {
    public boolean found = true;
    public String grant;
    public String consumer;
  }

  static GrantState parseState(String state) {
    return switch (state) {
      case "allocated" -> GrantState.ALLOCATED;
      case "loaded" -> GrantState.ALLOCATED_LOADED;
      case "active" -> GrantState.ACTIVE;
      case "revoked" -> GrantState.REVOKED;
      case "unloaded" -> GrantState.REVOKED_UNLOADED;
      default -> throw new IllegalArgumentException("unknown grant state: %s".formatted(state));
    };
  }

  private static void assertConsumersEqual(Instance c1, Instance c2, String message) {
    assertProtosEqual(c1.toProto(), c2.toProto(), message);
  }

  private static void assertGrantsEqual(GrantInfo g1, GrantInfo g2, String message) {
    assertProtosEqual(g1.toProto(), g2.toProto(), message);
  }

  @Test
  public void testUpdate_ChangeWithInvalidId() {
    ClusterMap c = new ClusterMap(CLUSTER_ID, List.of());
    assertEquals(c.getConsumers().size(), 0);

    var snapshot = new Messages.Snapshot(List.of(), newInstance, ImmutableList.of());
    var msg =
        new Messages.ClusterMessage(
            Optional.of(snapshot), Optional.empty(), newInstance.getId(), 133, Instant.now());

    c = c.update(msg);

    assertEquals(c.getConsumers().size(), 0);

    var change =
        new Messages.Change(
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    msg =
        new Messages.ClusterMessage(
            Optional.empty(), Optional.of(change), "id2", 135, Instant.now());

    try {
      c.update(msg);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unexpected incremental update"));
    }
  }

  @Test
  public void testUpdate_UnsupportedMessage() {
    ClusterMap c = new ClusterMap(CLUSTER_ID, List.of());
    assertEquals(c.getConsumers().size(), 0);

    var msg =
        new Messages.ClusterMessage(
            Optional.empty(), Optional.empty(), newInstance.getId(), 133, Instant.now());

    try {
      c.update(msg);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unsupported message"));
    }
  }
}
