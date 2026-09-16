package co.atoms.splitter.consumer.processor;

import io.grpc.stub.StreamObserver;

public interface GrpcRequestHandler<REQ, RESP> {
  void handle(REQ request, StreamObserver<RESP> responseObserver);
}
