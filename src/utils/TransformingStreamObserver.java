package co.atoms.splitter.utils;

import io.grpc.stub.StreamObserver;
import java.util.function.Function;

/** StreamObserver that transforms messages from another StreamObserver */
public class TransformingStreamObserver<T, E> implements StreamObserver<T> {

  private final StreamObserver<E> original;
  private final Function<T, E> transform;

  public TransformingStreamObserver(StreamObserver<E> original, Function<T, E> transform) {
    this.original = original;
    this.transform = transform;
  }

  @Override
  public void onNext(T t) {
    original.onNext(transform.apply(t));
  }

  @Override
  public void onError(Throwable throwable) {
    original.onError(throwable);
  }

  @Override
  public void onCompleted() {
    original.onCompleted();
  }
}
