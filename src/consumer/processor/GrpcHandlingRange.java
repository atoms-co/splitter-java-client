package co.atoms.splitter.consumer.processor;

import java.time.Duration;

/** {@link Range} that can handle requests. */
public abstract class GrpcHandlingRange<REQ, RESP> implements GrpcRequestHandler<REQ, RESP>, Range {
  @Override
  public void initialize() {}

  @Override
  public void activateAsync() {}

  @Override
  public void drain(Duration timeout) {}

  @Override
  public void terminateAsync() {}
}
