package co.atoms.splitter.consumer.processor.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class GrpcExceptions {
  public static StatusRuntimeException outOfRange(String message) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.OUT_OF_RANGE).withDescription(message));
  }

  public static StatusRuntimeException internal(String message) {
    return new StatusRuntimeException(
        Status.fromCode(Status.Code.INTERNAL).withDescription(message));
  }
}
