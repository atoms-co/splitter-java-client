package co.atoms.splitter.consumer.processor;

import static co.atoms.splitter.testing.Fixtures.DOMAIN2;
import static co.atoms.splitter.testing.Fixtures.SHARD_D2_GLOBAL_15;
import static co.atoms.splitter.testing.Fixtures.pad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import co.atoms.splitter.consumer.Ownership;
import co.atoms.splitter.consumer.processor.testing.TestRange;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.Shard;
import co.atoms.splitter.testing.TestStreamObserver;
import org.junit.jupiter.api.Test;

public class GrpcWorkProcessorTest {
  @Test
  public void testHandleGrpcRequest() {
    GrpcWorkProcessor<QualifiedDomainKey, QualifiedDomainKey, TestRange> wp =
        new GrpcWorkProcessor<>(
            new WorkProcessor<>((id, shard, ownership) -> new TestRange()) {
              @Override
              public void handleWork(String id, Shard shard, Ownership ownership) {}
            });
    wp.getRanges().activate("g1", SHARD_D2_GLOBAL_15, new TestRange());

    var outside = QualifiedDomainKey.create(DOMAIN2, pad("01"));
    assertFalse(wp.lookup(outside).isPresent());
    var inside = QualifiedDomainKey.create(DOMAIN2, pad("1"));
    assertTrue(wp.lookup(inside).isPresent());

    var resp = new TestStreamObserver<QualifiedDomainKey>();
    wp.handleGrpcRequest(inside, inside, resp);
    assertNull(resp.exception.get());
    assertTrue(resp.completed.get());
    assertEquals(resp.result.get(), inside);

    resp = new TestStreamObserver<>();
    wp.handleGrpcRequest(outside, outside, resp);
    assertEquals(
        resp.exception.get().getMessage(),
        "OUT_OF_RANGE: No owner found for key %s".formatted(outside));
    assertFalse(resp.completed.get());
    assertNull(resp.result.get());
  }
}
