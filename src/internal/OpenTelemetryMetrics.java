package co.atoms.splitter.internal;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.MeterProvider;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Factory methods for creating OTel-native metric instruments.
 *
 * <p><b>Initialization:</b> Do not call these methods from {@code static final} field initializers.
 * The OTel SDK must be fully initialized before instruments are created. Create instruments inside
 * constructors, {@code @Provides @Singleton} methods, or any code that runs after {@code
 * Application.run()}.
 *
 * <p>Example:
 *
 * <pre>{@code
 * @Provides
 * @Singleton
 * LongCounter provideRequestCounter() {
 *   return OpenTelemetryMetrics.createCounter("request_count", "Number of requests");
 * }
 * }</pre>
 */
public final class OpenTelemetryMetrics {

  private static final String METER_NAME = "co.atoms.stats";

  private static final Object PROVIDER_LOCK = new Object();

  private static final class ProviderState {
    @Nullable final MeterProvider provider;
    final long generation;

    ProviderState(@Nullable MeterProvider provider, long generation) {
      this.provider = provider;
      this.generation = generation;
    }
  }

  /**
   * Optional override for the {@link MeterProvider} that backs every instrument created here. When
   * {@code null} (the default), instruments bind to {@link GlobalOpenTelemetry}.
   */
  private static volatile ProviderState providerState = new ProviderState(null, 0);

  /**
   * Overrides the {@link MeterProvider} used to create instruments. Must be called before any
   * instrument is created for the override to take effect (instruments bind to a provider at
   * creation time). Passing {@code null} restores the default of {@link GlobalOpenTelemetry} for
   * ordinary instruments.
   */
  public static void setMeterProvider(@Nullable MeterProvider provider) {
    ProviderState newState;
    synchronized (PROVIDER_LOCK) {
      newState = new ProviderState(provider, providerState.generation + 1);
      providerState = newState;
    }
  }

  /**
   * Initializes Global OpenTelemetry and installs its meter provider, unless an explicit provider
   * is already installed.
   *
   * @return {@code true} if the global provider was installed, or {@code false} if an explicit
   *     provider was already present
   */
  public static boolean installGlobalMeterProviderIfAbsent() {
    if (providerState.provider != null) {
      return false;
    }

    // GlobalOpenTelemetry.get() may initialize the SDK. It is intentionally reached only through
    // this explicit readiness method, after the caller has configured autoconfiguration.
    MeterProvider globalProvider = GlobalOpenTelemetry.get().getMeterProvider();
    ProviderState newState;
    synchronized (PROVIDER_LOCK) {
      if (providerState.provider != null) {
        return false;
      }
      newState = new ProviderState(globalProvider, providerState.generation + 1);
      providerState = newState;
    }
    return true;
  }

  /** Creates a counter instrument. Use {@link LongCounter#add} to record increments. */
  public static LongCounter createCounter(String name, String description) {
    return meter().counterBuilder(name).setDescription(description).build();
  }

  /** Creates a long gauge instrument. Use {@link LongGauge#set} to record the current value. */
  public static LongGauge createGauge(String name, String description) {
    return meter().gaugeBuilder(name).ofLongs().setDescription(description).build();
  }

  /**
   * Creates a histogram instrument with custom bucket boundaries.
   */
  public static DoubleHistogram createHistogram(
      String name, String description, BucketOptions bucketOptions) {
    return createHistogram(
        name, description, BucketOptions.getBucketBoundaries(bucketOptions));
  }

  /**
   * Creates a histogram instrument with an explicit list of bucket boundaries.
   */
  public static DoubleHistogram createHistogram(
      String name, String description, List<Double> bucketBoundaries) {
    return meter()
        .histogramBuilder(name)
        .setDescription(description)
        .setExplicitBucketBoundariesAdvice(bucketBoundaries)
        .build();
  }


  private static io.opentelemetry.api.metrics.Meter meter() {
    MeterProvider provider = providerState.provider;
    return provider != null ? provider.get(METER_NAME) : GlobalOpenTelemetry.getMeter(METER_NAME);
  }

  private OpenTelemetryMetrics() {}
}
