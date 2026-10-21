package co.atoms.splitter.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Location;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PeerChannelsTest {

  private static Instance instance(String id) {
    return Instance.create(id, Location.LOCAL, Instant.EPOCH, id, "localhost:1");
  }

  private static PeerChannels newPeerChannels() {
    return new PeerChannels(
        target -> ManagedChannelBuilder.forTarget(target).usePlaintext().build());
  }

  @Test
  void retainClosesDepartedChannels() throws Exception {
    var channels = newPeerChannels();

    var kept = instance("kept");
    var departed = instance("departed");
    var keptChannel = channels.channel(kept);
    var departedChannel = channels.channel(departed);
    assertSame(keptChannel, channels.channel(kept));

    channels.retain(List.of(kept));

    // Caffeine runs removal listeners asynchronously.
    awaitShutdown(departedChannel);
    assertFalse(keptChannel.isShutdown());
    assertSame(keptChannel, channels.channel(kept));

    // A fresh channel is dialed when a departed consumer is accessed again.
    var redialed = channels.channel(departed);
    assertNotSame(departedChannel, redialed);

    channels.retain(List.of());
    awaitShutdown(keptChannel);
    awaitShutdown(redialed);
  }

  @Test
  void retainWithoutCachedChannelsIsNoop() {
    var channels = newPeerChannels();
    channels.retain(List.of(instance("unknown")));
  }

  private static void awaitShutdown(ManagedChannel channel) throws InterruptedException {
    for (int i = 0; i < 1000 && !channel.isShutdown(); i++) {
      Thread.sleep(10);
    }
    assertTrue(channel.isShutdown());
  }
}
