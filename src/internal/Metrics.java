package co.atoms.splitter.internal;

import co.atoms.splitter.consumer.Consumer;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainName;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import javax.annotation.Nullable;

/**
 * Splitter java client metrics, recorded through OTel-native instruments.
 */
public class Metrics {
  private Metrics() {}

  private static final String SOURCE = "splitter-java-client";

  private static final AttributeKey<String> TENANT_KEY = AttributeKey.stringKey("tenant");
  private static final AttributeKey<String> SERVICE_KEY = AttributeKey.stringKey("service");
  private static final AttributeKey<String> DOMAIN_KEY = AttributeKey.stringKey("domain");
  private static final AttributeKey<String> LEASE_STATE_KEY = AttributeKey.stringKey("lease_state");
  private static final AttributeKey<String> RESULT_KEY = AttributeKey.stringKey("result");
  private static final AttributeKey<String> HANDLER_KEY = AttributeKey.stringKey("handler");
  private static final AttributeKey<String> SOURCE_KEY = AttributeKey.stringKey("source");
  private static final AttributeKey<String> SOURCE_VERSION_KEY =
      AttributeKey.stringKey("source_version");
  private static final AttributeKey<String> LOCATION_KEY = AttributeKey.stringKey("location");

  private static final String SHARDS_NAME = "co_atoms_splitter_client_workpool_grants";
  private static final String FORWARDED_REQUESTS_NAME = "co_atoms_splitter_client_forwarded_requests";
  private static final String HANDLED_REQUESTS_NAME = "co_atoms_splitter_client_handled_requests";
  private static final String GRANT_DURATION_NAME = "co_atoms_splitter_client_grant_duration";

  private static final BucketOptions GRANT_DURATION_BUCKETS =
      BucketOptions.of(
          "s",
          1d,
          5d,
          60d,
          600d, // 10m
          1800d, // 30m
          3600d, // 1h
          21_600d, // 6h
          57_600d, // 12h
          86_400d, // 24h
          345_600d, // 3d
          604_800d // 1w
          );

  @Nullable private static volatile Instruments instruments;

  /**
   * Lazily builds the instruments on first use. They must not be created in {@code static}
   * initializers: the OTel SDK must be initialized first and instruments bind to the meter provider
   * current at creation time (see {@link OpenTelemetryMetrics}). First use here is the first
   * recorded observation, which is at request time — well after application startup.
   */
  private static Instruments instruments() {
    Instruments local = instruments;
    if (local == null) {
      synchronized (Metrics.class) {
        local = instruments;
        if (local == null) {
          local = new Instruments();
          instruments = local;
        }
      }
    }
    return local;
  }

  @VisibleForTesting
  static void resetMetricsForTest() {
    instruments = null;
  }

  private static final class Instruments {
    final LongGauge shards =
        OpenTelemetryMetrics.createGauge(SHARDS_NAME, "Number of assigned shards");
    final LongCounter forwardedRequests =
        OpenTelemetryMetrics.createCounter(FORWARDED_REQUESTS_NAME, "Number of forwarded requests");
    final LongCounter handledRequests =
        OpenTelemetryMetrics.createCounter(
            HANDLED_REQUESTS_NAME, "Number of requests handled locally");
    final DoubleHistogram grantDuration =
        OpenTelemetryMetrics.createHistogram(
            GRANT_DURATION_NAME, "Completed grant duration", GRANT_DURATION_BUCKETS);
  }

  public static void recordShards(QualifiedDomainName domainName, String state, long value) {
    instruments()
        .shards
        .set(
            value,
            Attributes.builder()
                .put(TENANT_KEY, domainName.service().tenant())
                .put(SERVICE_KEY, domainName.service().name())
                .put(DOMAIN_KEY, domainName.name())
                .put(LEASE_STATE_KEY, state.toLowerCase())
                .put(SOURCE_KEY, SOURCE)
                .put(SOURCE_VERSION_KEY, Consumer.CLIENT_VERSION)
                .build());
  }

  public static void recordForwardedRequest(
      QualifiedDomainName domainName, String handler, String result, Location location) {
    instruments()
        .forwardedRequests
        .add(1, requestAttributes(domainName, handler, result, location));
  }

  public static void recordHandledRequest(
      QualifiedDomainName domainName, String handler, String result, Location location) {
    instruments().handledRequests.add(1, requestAttributes(domainName, handler, result, location));
  }

  public static void recordDeletedGrantDuration(QualifiedDomainName domainName, long value) {
    instruments()
        .grantDuration
        .record(
            (double) value,
            Attributes.builder()
                .put(TENANT_KEY, domainName.service().tenant())
                .put(SERVICE_KEY, domainName.service().name())
                .put(DOMAIN_KEY, domainName.name())
                .put(SOURCE_KEY, SOURCE)
                .put(SOURCE_VERSION_KEY, Consumer.CLIENT_VERSION)
                .build());
  }

  private static Attributes requestAttributes(
      QualifiedDomainName domainName, String handler, String result, Location location) {
    return Attributes.builder()
        .put(TENANT_KEY, domainName.service().tenant())
        .put(SERVICE_KEY, domainName.service().name())
        .put(DOMAIN_KEY, domainName.name())
        .put(RESULT_KEY, result)
        .put(HANDLER_KEY, handler)
        .put(SOURCE_KEY, SOURCE)
        .put(SOURCE_VERSION_KEY, Consumer.CLIENT_VERSION)
        .put(LOCATION_KEY, location.toString())
        .build();
  }
}
