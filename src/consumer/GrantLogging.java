package co.atoms.splitter.consumer;

import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.model.Shard;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import net.logstash.logback.marker.Markers;
import org.slf4j.Logger;

/**
 * Emits structured grant event and current-state logs.
 */
final class GrantLogging {

  private static final int MAX_GRANTS_PER_LOG = 64;

  enum EventType {
    ASSIGN,
    PROMOTE,
    REVOKE,
    UPDATE,
    REMOVE,
    CHECKPOINT;

    String logValue() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  record GrantEvent(
      EventType type,
      Instant time,
      QualifiedServiceName service,
      Instance consumer,
      Grant grant,
      @Nullable GrantState fromState,
      @Nullable GrantState toState,
      String clientVersion) {}

  record GrantSnapshot(String grant, GrantState state, Instant assignedAt) {}

  record ShardSnapshot(Shard shard, List<GrantSnapshot> grants) {}

  record CurrentGrants(
      QualifiedServiceName service,
      Instance consumer,
      boolean coordinatorConnected,
      boolean draining,
      String clientVersion,
      List<Grant> grants) {}

  private GrantLogging() {}

  static void logGrantEvent(Logger logger, String message, GrantEvent event) {
    logger.info(Markers.appendEntries(eventFields(event)), message);
  }

  static void logGrants(Logger logger, String message, CurrentGrants currentGrants, Instant at) {
    var id = UUID.randomUUID().toString();
    for (var fields : fieldsByPart(currentGrants, id, at)) {
      logger.info(Markers.appendEntries(fields), message);
    }
  }

  static Map<String, Object> eventFields(GrantEvent event) {
    var grant = event.grant();
    var shard = grant.shard();
    var fields = baseFields(event.service(), event.consumer(), event.clientVersion());
    fields.put("time", event.time().toString());
    fields.put("event_type", event.type().logValue());
    addShardFields(fields, shard);
    fields.put("grant", grant.id());
    fields.put("worker", event.consumer().id());
    if (event.fromState() != null) {
      fields.put("from_state", state(event.fromState()));
    }
    if (event.toState() != null) {
      fields.put("to_state", state(event.toState()));
    }
    fields.put("assigned_at", grant.assigned().toString());
    return fields;
  }

  static List<Map<String, Object>> fieldsByPart(
      CurrentGrants currentGrants, String id, Instant at) {
    var parts = splitShards(grantsByShard(currentGrants.grants()));
    var fieldsByPart = new ArrayList<Map<String, Object>>(parts.size());
    for (int partIndex = 0; partIndex < parts.size(); partIndex++) {
      fieldsByPart.add(
          partFields(currentGrants, id, at, partIndex, parts.size(), parts.get(partIndex)));
    }
    return fieldsByPart;
  }

  private static Map<String, Object> partFields(
      CurrentGrants currentGrants,
      String id,
      Instant at,
      int partIndex,
      int partCount,
      List<ShardSnapshot> shards) {
    var fields =
        baseFields(
            currentGrants.service(), currentGrants.consumer(), currentGrants.clientVersion());
    fields.put("time", at.toString());
    fields.put("event_type", EventType.CHECKPOINT.logValue());
    fields.put("checkpoint_id", id);
    fields.put("checkpoint_at", at.toString());
    fields.put("checkpoint_part_index", partIndex);
    fields.put("checkpoint_part_count", partCount);

    var logShards = new ArrayList<Map<String, Object>>(shards.size());
    for (var shardSnapshot : shards) {
      var shard = shardSnapshot.shard();
      var logShard = new LinkedHashMap<String, Object>();
      addShardFields(logShard, shard);

      var logGrants = new ArrayList<Map<String, Object>>(shardSnapshot.grants().size());
      for (var grantSnapshot : shardSnapshot.grants()) {
        var logGrant = new LinkedHashMap<String, Object>();
        logGrant.put("grant", grantSnapshot.grant());
        logGrant.put("worker", currentGrants.consumer().id());
        logGrant.put("consumer_region", currentGrants.consumer().location().region());
        logGrant.put("consumer_node", currentGrants.consumer().location().node());
        logGrant.put("state", state(grantSnapshot.state()));
        logGrant.put("assigned_at", grantSnapshot.assignedAt().toString());
        logGrants.add(logGrant);
      }
      logShard.put("grants", logGrants);
      logShards.add(logShard);
    }

    fields.put("shards", logShards);
    fields.put("worker", currentGrants.consumer().id());
    fields.put("coordinator_connected", currentGrants.coordinatorConnected());
    fields.put("draining", currentGrants.draining());
    return fields;
  }

  private static List<ShardSnapshot> grantsByShard(List<Grant> grants) {
    var grantsByShard = new HashMap<Shard, List<GrantSnapshot>>();
    for (var grant : grants) {
      grantsByShard
          .computeIfAbsent(grant.shard(), unused -> new ArrayList<>())
          .add(new GrantSnapshot(grant.id(), grant.state(), grant.assigned()));
    }

    var shards = new ArrayList<ShardSnapshot>(grantsByShard.size());
    grantsByShard.forEach(
        (shard, snapshots) -> shards.add(new ShardSnapshot(shard, List.copyOf(snapshots))));
    return shards;
  }

  static List<List<ShardSnapshot>> splitShards(List<ShardSnapshot> shards) {
    var parts = new ArrayList<List<ShardSnapshot>>();
    var part = new ArrayList<ShardSnapshot>();
    int grantCount = 0;
    for (var shard : shards) {
      int shardGrantCount = shard.grants().size();
      if (!part.isEmpty() && grantCount + shardGrantCount > MAX_GRANTS_PER_LOG) {
        parts.add(List.copyOf(part));
        part = new ArrayList<>();
        grantCount = 0;
      }
      part.add(shard);
      grantCount += shardGrantCount;
    }
    if (!part.isEmpty()) {
      parts.add(List.copyOf(part));
    }
    if (parts.isEmpty()) {
      parts.add(List.of());
    }
    return parts;
  }

  private static LinkedHashMap<String, Object> baseFields(
      QualifiedServiceName service, Instance consumer, String clientVersion) {
    var fields = new LinkedHashMap<String, Object>();
    fields.put("event_source", "consumer");
    fields.put("tenant", service.tenant());
    fields.put("service", service.name());
    fields.put("consumer_id", consumer.id());
    fields.put("consumer_region", consumer.location().region());
    fields.put("consumer_node", consumer.location().node());
    fields.put("consumer_name", consumer.name());
    fields.put("client_language", "java");
    fields.put("client_version", clientVersion);
    return fields;
  }

  private static void addShardFields(Map<String, Object> fields, Shard shard) {
    fields.put("domain", shard.domain().name());
    fields.put("shard_region", shard.region());
    fields.put("shard_from", shard.from().toString());
    fields.put("shard_to", shard.to().toString());
  }

  private static String state(GrantState state) {
    return state.name().toLowerCase(Locale.ROOT);
  }
}
