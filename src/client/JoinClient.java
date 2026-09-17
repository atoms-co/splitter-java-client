package co.atoms.splitter.client;

import co.atoms.splitter.proto.ConsumerServiceGrpc;
import co.atoms.splitter.proto.JoinMessage;
import io.grpc.stub.StreamObserver;

@FunctionalInterface
public interface JoinClient {
  StreamObserver<JoinMessage> join(StreamObserver<JoinMessage> request);

  static JoinClient from(ConsumerServiceGrpc.ConsumerServiceStub stub) {
    return stub::join;
  }
}
