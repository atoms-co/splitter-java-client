package co.atoms.splitter.consumer.testing;

import co.atoms.splitter.cluster.Cluster;
import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.testing.Gate;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestOwnership implements Ownership {

  private Instant expiration;
  private GrantState state = GrantState.UNKNOWN;
  private final Object lock = new Object();

  public final Gate waitForOtherGrantToUnload = new Gate();
  public final AtomicBoolean otherUnloaded = new AtomicBoolean();
  public final AtomicBoolean loaded = new AtomicBoolean();
  public final Gate waitingForActive = new Gate();
  public final Gate waitForActive = new Gate();
  public final AtomicBoolean active = new AtomicBoolean();
  public final Gate waitingForRevoked = new Gate();
  public final Gate waitForRevoked = new Gate();
  public final AtomicBoolean revoked = new AtomicBoolean();
  public final AtomicBoolean unloaded = new AtomicBoolean();
  public final Gate waitForOtherGrantToLoad = new Gate();
  public final AtomicBoolean otherLoaded = new AtomicBoolean();
  public final AtomicBoolean onTermination = new AtomicBoolean();
  public final AtomicBoolean terminated = new AtomicBoolean();

  public TestOwnership(Instant expiration) {
    this.expiration = expiration;
  }

  @Override
  public Instant getExpiration() {
    synchronized (lock) {
      return expiration;
    }
  }

  public void setExpiration(Instant expiration) {
    synchronized (lock) {
      this.expiration = expiration;
    }
  }

  @Override
  public GrantState getState() {
    synchronized (lock) {
      return state;
    }
  }

  public void setState(GrantState state) {
    synchronized (lock) {
      this.state = state;
    }
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }

  @Override
  public boolean waitForOtherGrantToUnload() {
    waitForOtherGrantToUnload.await();
    return otherUnloaded.get();
  }

  @Override
  public void load() {
    loaded.set(true);
  }

  @Override
  public boolean waitForActive() {
    waitingForActive.open();
    waitForActive.await();
    return active.get();
  }

  @Override
  public boolean waitForRevoked() {
    waitingForRevoked.open();
    waitForRevoked.await();
    return revoked.get();
  }

  @Override
  public void unload() {
    unloaded.set(true);
  }

  @Override
  public boolean waitForOtherGrantToLoad() {
    waitForOtherGrantToLoad.await();
    return otherLoaded.get();
  }

  @Override
  public void onTermination(Runnable r) {
    onTermination.set(true);
  }

  @Override
  public void release() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Cluster getCluster() {
    throw new UnsupportedOperationException();
  }
}
