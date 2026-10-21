package co.atoms.splitter.consumer;

import co.atoms.splitter.model.Instance;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.grpc.ManagedChannel;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channels to other consumers in the cluster. Channels are dialed on first use, reused, and closed
 * when their consumer leaves the cluster.
 */
public class PeerChannels {

  private static final Logger LOGGER = LoggerFactory.getLogger(PeerChannels.class);

  private final LoadingCache<Instance, ManagedChannel> channels;

  /**
   * @param dial Creates a channel for a consumer endpoint. Injected so applications choose the
   *     channel builder (e.g. one with interceptors) and this class stays platform-agnostic.
   */
  public PeerChannels(Function<String, ManagedChannel> dial) {
    this.channels =
        Caffeine.newBuilder()
            .recordStats()
            .maximumSize(100)
            .<Instance, ManagedChannel>removalListener((k, channel, cause) -> shutdown(channel))
            .build(consumer -> dial.apply(consumer.endpoint()));
  }

  /** Returns a shared channel to the consumer, dialing on first use. */
  public ManagedChannel channel(Instance consumer) {
    return channels.get(consumer);
  }

  /** Closes channels to consumers no longer in the cluster. */
  public void retain(Collection<Instance> consumers) {
    channels.asMap().keySet().retainAll(Set.copyOf(consumers));
  }

  /** Closes all channels. */
  public void close() {
    channels.invalidateAll();
  }

  private static void shutdown(ManagedChannel channel) {
    try {
      channel.shutdown();
    } catch (Exception error) {
      LOGGER.error("Failed to shut down channel", error);
    }
  }
}
