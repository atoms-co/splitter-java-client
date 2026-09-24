package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.model.Instance;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/**
 * Cache for gRPC stubs to avoid creating new channels on each request. Stubs are created using the
 * provided factory method and are cached for some time after last access.
 */
public class GrpcStubsCache<STUB> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcStubsCache.class);

  private final LoadingCache<Instance, StubWithChannel<STUB>> stubs;
  private final Function<ManagedChannel, STUB> stubFactory;
  private final Function<String, ManagedChannel> channelFactory;

  public GrpcStubsCache(Function<ManagedChannel, STUB> stubFactory, Function<String, ManagedChannel> channelFactory) {
    this.stubFactory = stubFactory;
    this.channelFactory = channelFactory;
    stubs =
        Caffeine.newBuilder()
            .recordStats()
            .maximumSize(100)
            .expireAfterAccess(Duration.ofMinutes(10))
            .<Instance, StubWithChannel<STUB>>removalListener(
                (k, value, c) -> this.onCacheRemoval(value))
            .build(this::buildStub);
  }

  public GrpcStubsCache(Function<ManagedChannel, STUB> stubFactory) {
    this(stubFactory, t -> NettyChannelBuilder.forTarget(t).build());
  }

  public STUB getStub(Instance consumer) {
    return stubs.get(consumer).stub;
  }

  private StubWithChannel<STUB> buildStub(Instance instance) {
    var channel = channelFactory.apply(instance.endpoint());
    return new StubWithChannel<>(channel, stubFactory.apply(channel));
  }

  private void onCacheRemoval(StubWithChannel<STUB> stub) {
    try {
      stub.channel.shutdown();
    } catch (Exception error) {
      LOGGER.error("Failed to shut down channel", error);
    }
  }

  private static class StubWithChannel<STUB> {
    final ManagedChannel channel;
    final STUB stub;

    public StubWithChannel(ManagedChannel channel, STUB stub) {
      this.channel = channel;
      this.stub = stub;
    }
  }
}
