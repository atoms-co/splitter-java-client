package co.atoms.splitter.consumer;

import static co.atoms.splitter.model.GrantState.ACTIVE;
import static co.atoms.splitter.model.GrantState.ALLOCATED_LOADED;
import static co.atoms.splitter.testing.Fixtures.SHARD_D1_UNIT_0e;
import static co.atoms.splitter.testing.Fixtures.TS;
import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedServiceName;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class GrantLoggingTest {

  @Test
  public void splitsShardsWithoutExceedingGrantLimit() {
    assertEquals(List.of(List.of()), grantCountsByPart(List.of()));
    assertEquals(List.of(List.of(20, 20)), grantCountsByPart(List.of(20, 20)));
    assertEquals(List.of(List.of(32, 32)), grantCountsByPart(List.of(32, 32)));
    assertEquals(List.of(List.of(63), List.of(2, 1)), grantCountsByPart(List.of(63, 2, 1)));
  }

  @Test
  public void createsGrantEventFields() {
    var event =
        new GrantLogging.GrantEvent(
            GrantLogging.EventType.PROMOTE,
            TS,
            service(),
            consumer(),
            grant(),
            ALLOCATED_LOADED,
            ACTIVE,
            Consumer.CLIENT_VERSION);

    assertEquals(
        Map.ofEntries(
            entry("event_source", "consumer"),
            entry("time", TS.toString()),
            entry("tenant", "tenant"),
            entry("service", "service"),
            entry("consumer_id", "worker-id"),
            entry("consumer_region", "consumer-region"),
            entry("consumer_node", "consumer-node"),
            entry("consumer_name", "consumer-name"),
            entry("client_language", "java"),
            entry("client_version", Consumer.CLIENT_VERSION),
            entry("event_type", "promote"),
            entry("domain", "domain1"),
            entry("shard_region", ""),
            entry("shard_from", SHARD_D1_UNIT_0e.from().toString()),
            entry("shard_to", SHARD_D1_UNIT_0e.to().toString()),
            entry("grant", "grant-id"),
            entry("worker", "worker-id"),
            entry("from_state", "allocated_loaded"),
            entry("to_state", "active"),
            entry("assigned_at", TS.toString())),
        GrantLogging.eventFields(event));
  }

  @Test
  public void createsCheckpointFields() {
    var currentGrants =
        new GrantLogging.CurrentGrants(
            service(), consumer(), true, false, Consumer.CLIENT_VERSION, List.of(grant()));

    assertEquals(
        List.of(
            Map.ofEntries(
                entry("event_source", "consumer"),
                entry("time", TS.toString()),
                entry("tenant", "tenant"),
                entry("service", "service"),
                entry("consumer_id", "worker-id"),
                entry("consumer_region", "consumer-region"),
                entry("consumer_node", "consumer-node"),
                entry("consumer_name", "consumer-name"),
                entry("client_language", "java"),
                entry("client_version", Consumer.CLIENT_VERSION),
                entry("event_type", "checkpoint"),
                entry("checkpoint_id", "checkpoint-id"),
                entry("checkpoint_at", TS.toString()),
                entry("checkpoint_part_index", 0),
                entry("checkpoint_part_count", 1),
                entry(
                    "shards",
                    List.of(
                        Map.of(
                            "domain",
                            "domain1",
                            "shard_region",
                            "",
                            "shard_from",
                            SHARD_D1_UNIT_0e.from().toString(),
                            "shard_to",
                            SHARD_D1_UNIT_0e.to().toString(),
                            "grants",
                            List.of(
                                Map.of(
                                    "grant",
                                    "grant-id",
                                    "worker",
                                    "worker-id",
                                    "consumer_region",
                                    "consumer-region",
                                    "consumer_node",
                                    "consumer-node",
                                    "state",
                                    "active",
                                    "assigned_at",
                                    TS.toString()))))),
                entry("worker", "worker-id"),
                entry("coordinator_connected", true),
                entry("draining", false))),
        GrantLogging.fieldsByPart(currentGrants, "checkpoint-id", TS));
  }

  private static List<List<Integer>> grantCountsByPart(List<Integer> grantsPerShard) {
    var shards = grantsPerShard.stream().map(GrantLoggingTest::shardWithGrants).toList();
    return GrantLogging.splitShards(shards).stream()
        .map(part -> part.stream().map(shard -> shard.grants().size()).toList())
        .toList();
  }

  private static GrantLogging.ShardSnapshot shardWithGrants(int grantCount) {
    var grant = new GrantLogging.GrantSnapshot("grant", ACTIVE, TS);
    return new GrantLogging.ShardSnapshot(SHARD_D1_UNIT_0e, Collections.nCopies(grantCount, grant));
  }

  private static QualifiedServiceName service() {
    return QualifiedServiceName.create("tenant", "service");
  }

  private static Instance consumer() {
    return Instance.create(
        "worker-id",
        Location.create("consumer-region", "consumer-node"),
        TS,
        "consumer-name",
        "consumer-endpoint");
  }

  private static Grant grant() {
    return Grant.create("grant-id", SHARD_D1_UNIT_0e, ACTIVE, TS.plusSeconds(1), TS);
  }
}
