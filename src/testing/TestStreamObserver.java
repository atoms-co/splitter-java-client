package co.atoms.splitter.testing;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestStreamObserver<T> implements StreamObserver<T> {
  public final AtomicReference<T> result = new AtomicReference<>();
  public final AtomicBoolean completed = new AtomicBoolean();
  public final AtomicReference<Throwable> exception = new AtomicReference<>();

  @Override
  public void onNext(T value) {
    result.set(value);
  }

  @Override
  public void onError(Throwable t) {
    exception.set(t);
  }

  @Override
  public void onCompleted() {
    completed.set(true);
  }
}
