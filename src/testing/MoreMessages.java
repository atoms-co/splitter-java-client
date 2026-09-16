package co.atoms.splitter.testing;

import co.atoms.lib.net.session.proto.Message;
import co.atoms.splitter.cluster.GrantInfo;
import co.atoms.splitter.cluster.Messages;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.proto.ClientMessage;
import co.atoms.splitter.proto.ClusterMessage;
import co.atoms.splitter.proto.ConsumerMessage;
import co.atoms.splitter.proto.JoinMessage;
import co.atoms.splitter.utils.TimeUtils;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MoreMessages {

  public static Message newEstablished(Clock clock, co.atoms.lib.net.location.proto.Instance server) {
    return Message.newBuilder()
        .setEstablished(
            Message.Established.newBuilder()
                .setTtl(TimeUtils.toTimestamp(Instant.now(clock)))
                .setServer(server)
                .build())
        .build();
  }

  public static JoinMessage newClusterSnapshot(
      co.atoms.lib.net.location.proto.Instance server,
      List<Shard> shards,
      co.atoms.splitter.cluster.Messages.Assignment... assignments) {
    var snapshot =
        ClusterMessage.Snapshot.newBuilder()
            .setOrigin(server)
            .addAllAssignments(
                Arrays.stream(assignments)
                    .map(co.atoms.splitter.cluster.Messages.Assignment::toProto)
                    .collect(Collectors.toList()))
            .addAllShards(shards.stream().map(Shard::toProto).collect(Collectors.toList()));
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setCluster(ClusterMessage.newBuilder().setSnapshot(snapshot).build()))
        .build();
  }

  public static JoinMessage newClusterAssign(
      String id, int version, co.atoms.splitter.cluster.Messages.Assignment... assignments) {
    var assign =
        ClusterMessage.Assign.newBuilder()
            .addAllAssignments(
                Arrays.stream(assignments)
                    .map(co.atoms.splitter.cluster.Messages.Assignment::toProto)
                    .collect(Collectors.toList()));
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setCluster(
                    ClusterMessage.newBuilder()
                        .setId(id)
                        .setVersion(version)
                        .setChange(ClusterMessage.Change.newBuilder().setAssign(assign).build())
                        .build()))
        .build();
  }

  public static JoinMessage newAssign(Grant... grants) {
    var g = Arrays.stream(grants).map(Grant::toProto).collect(Collectors.toList());
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setAssign(ClientMessage.Assign.newBuilder().addAllGrants(g))
                        .build()))
        .build();
  }

  public static JoinMessage newExtends(Instant lease) {
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setExtend(
                            ClientMessage.Extend.newBuilder()
                                .setLease(TimeUtils.toTimestamp(lease)))
                        .build()))
        .build();
  }

  public static JoinMessage newPromote(Grant... grants) {
    var g = Arrays.stream(grants).map(Grant::toProto).collect(Collectors.toList());
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setPromote(ClientMessage.Promote.newBuilder().addAllGrants(g))
                        .build()))
        .build();
  }

  public static JoinMessage newRevoke(Grant... grants) {
    var g = Arrays.stream(grants).map(Grant::toProto).collect(Collectors.toList());
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setRevoke(ClientMessage.Revoke.newBuilder().addAllGrants(g))
                        .build()))
        .build();
  }

  public static JoinMessage newNotify(Grant update, Grant target) {
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setNotify(
                            ClientMessage.Notify.newBuilder()
                                .setUpdate(update.toProto())
                                .setTarget(target.toProto()))
                        .build()))
        .build();
  }

  public static Messages.Assignment newAssignment(Instance consumer, GrantInfo... grants) {
    return Messages.Assignment.create(consumer, Arrays.asList(grants));
  }
}
