package co.atoms.splitter.consumer;

import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.QualifiedServiceName;
import co.atoms.splitter.proto.ClientMessage;
import co.atoms.splitter.proto.ConsumerMessage;
import co.atoms.splitter.proto.JoinMessage;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class Messages {
  public static ClientMessage.Register.Builder createRegisterBuilder(
      Instance instance, QualifiedServiceName service, Stream<Grant> grants) {
    return ClientMessage.Register.newBuilder()
        .setConsumer(instance.toProto())
        .setService(service.toProto())
        .addAllActive(grants.map(Grant::toProto).collect(Collectors.toList()));
  }

  public static JoinMessage createRegister(ClientMessage.Register register) {
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(ClientMessage.newBuilder().setRegister(register)))
        .build();
  }

  public static JoinMessage createDeregister() {
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setDeregister(ClientMessage.Deregister.getDefaultInstance())))
        .build();
  }

  public static JoinMessage createReleased(Grant... grants) {
    var protos = Arrays.stream(grants).map(Grant::toProto).toList();
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setReleased(
                            ClientMessage.Released.newBuilder().addAllGrants(protos).build())))
        .build();
  }

  public static JoinMessage createUpdate(Grant grant) {
    return JoinMessage.newBuilder()
        .setConsumer(
            ConsumerMessage.newBuilder()
                .setClient(
                    ClientMessage.newBuilder()
                        .setUpdate(ClientMessage.Update.newBuilder().setGrant(grant.toProto()))))
        .build();
  }

  public static JoinMessage wrapSessionMessage(co.atoms.lib.net.session.proto.Message msg) {
    return JoinMessage.newBuilder().setSession(msg).build();
  }

  public static @Nullable co.atoms.lib.net.session.proto.Message unwrapSessionMessage(JoinMessage msg) {
    if (msg.getMsgCase() != JoinMessage.MsgCase.SESSION) {
      return null;
    }
    return msg.getSession();
  }
}
