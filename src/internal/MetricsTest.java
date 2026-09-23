package co.atoms.splitter.internal;

import static com.google.common.truth.Truth.assertThat;

import co.atoms.splitter.internal.testing.OtelTestMetrics;
import co.atoms.splitter.consumer.Consumer;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainName;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MetricsTest {

  private static final QualifiedDomainName DOMAIN =
      QualifiedDomainName.parse("test-tenant/test-service/test-domain");

  @Test
  void recordsWorkpoolGrantsGauge() {
    try (OtelTestMetrics metrics = OtelTestMetrics.create()) {
      // Drop any instruments cached by a previous test so they rebind to this test's provider.
      Metrics.resetMetricsForTest();

      Metrics.recordShards(DOMAIN, "GRANTED", 7);

      assertThat(
              metrics.longGauge(
                  "co_atoms_splitter_client_workpool_grants",
                  Map.of(
                      "tenant", "test-tenant",
                      "service", "test-service",
                      "domain", "test-domain",
                      "lease_state", "granted",
                      "source", "splitter-java-client",
                      "source_version", Consumer.CLIENT_VERSION)))
          .isEqualTo(7L);
    }
  }

  @Test
  void recordsForwardedAndHandledCounters() {
    try (OtelTestMetrics metrics = OtelTestMetrics.create()) {
      Metrics.resetMetricsForTest();

      Location location = Location.create("us", "node-1");
      Metrics.recordForwardedRequest(DOMAIN, "myhandler", "ok", location);
      Metrics.recordForwardedRequest(DOMAIN, "myhandler", "ok", location);
      Metrics.recordHandledRequest(DOMAIN, "myhandler", "ok", location);

      Map<String, String> labels =
          Map.of(
              "tenant", "test-tenant",
              "service", "test-service",
              "domain", "test-domain",
              "result", "ok",
              "handler", "myhandler",
              "source", "splitter-java-client",
              "source_version", Consumer.CLIENT_VERSION,
              "location", "us/node-1");
      assertThat(metrics.longSum("co_atoms_splitter_client_forwarded_requests", labels)).isEqualTo(2L);
      assertThat(metrics.longSum("co_atoms_splitter_client_handled_requests", labels)).isEqualTo(1L);
    }
  }

  @Test
  void recordsGrantDurationHistogram() {
    try (OtelTestMetrics metrics = OtelTestMetrics.create()) {
      Metrics.resetMetricsForTest();

      Metrics.recordDeletedGrantDuration(DOMAIN, 42);

      Map<String, String> labels =
          Map.of(
              "tenant", "test-tenant",
              "service", "test-service",
              "domain", "test-domain",
              "source", "splitter-java-client",
              "source_version", Consumer.CLIENT_VERSION);
      assertThat(metrics.histogramCount("co_atoms_splitter_client_grant_duration", labels))
          .isEqualTo(1L);
      assertThat(metrics.histogramSum("co_atoms_splitter_client_grant_duration", labels))
          .isWithin(1e-9)
          .of(42.0);
    }
  }
}
