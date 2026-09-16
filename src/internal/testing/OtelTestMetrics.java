package co.atoms.splitter.internal.testing;

import co.atoms.splitter.internal.OpenTelemetryMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * In-memory OTel metrics harness for tests. This is the OTel-native counterpart to {@link
 * TestRecorders} (which mocks the legacy OpenCensus {@code Stats.recorder()} path).
 *
 * <p>On {@link #create()} it registers a fresh {@link SdkMeterProvider} backed by an in-memory
 * reader as the {@link OpenTelemetryMetrics} override, so any instrument created <b>after</b> this
 * call feeds the reader. Create instruments (i.e. the code under test) only after calling {@link
 * #create()}, and always {@link #close()} the harness (it resets the override and shuts the
 * provider down) so instruments in later tests don't bind to a stale provider.
 *
 * <pre>{@code
 * try (OtelTestMetrics metrics = OtelTestMetrics.create()) {
 *   MyThing thing = new MyThing(); // creates OpenTelemetryMetrics instruments here
 *   thing.doWork();
 *   assertThat(metrics.longSum("my_metric_sum", Map.of("status", "ok"))).isEqualTo(1L);
 * }
 * }</pre>
 */
public final class OtelTestMetrics implements AutoCloseable {
  private final SdkMeterProvider provider;
  private final InMemoryReader reader = new InMemoryReader();

  private OtelTestMetrics() {
    provider = SdkMeterProvider.builder().registerMetricReader(reader).build();
    OpenTelemetryMetrics.setMeterProvider(provider);
  }

  public static OtelTestMetrics create() {
    return new OtelTestMetrics();
  }

  public Collection<MetricData> collect() {
    return reader.collectAllMetrics();
  }

  /**
   * @return the summed value of the monotonic long-sum series {@code name} across every point whose
   *     attributes contain the given {@code labels} (empty map matches all points). Counter series
   *     created via {@link OpenTelemetryMetrics#createCounter} or the {@code _count}/{@code _sum}
   *     fan-out of {@link OpenTelemetryMetrics#createTriple} are long-sum series.
   */
  public long longSum(String name, Map<String, String> labels) {
    return metric(name)
        .filter(m -> m.getType() == io.opentelemetry.sdk.metrics.data.MetricDataType.LONG_SUM)
        .stream()
        .flatMap(m -> m.getLongSumData().getPoints().stream())
        .filter(p -> matches(p, labels))
        .mapToLong(LongPointData::getValue)
        .sum();
  }

  /**
   * @return the value of the long-gauge series {@code name} for the first point matching {@code
   *     labels}, or {@code 0} if none. The {@code _last} series of {@link
   *     OpenTelemetryMetrics#createTriple} and {@link OpenTelemetryMetrics#createGauge} are
   *     long-gauge series.
   */
  public long longGauge(String name, Map<String, String> labels) {
    return metric(name)
        .filter(m -> m.getType() == io.opentelemetry.sdk.metrics.data.MetricDataType.LONG_GAUGE)
        .stream()
        .flatMap(m -> m.getLongGaugeData().getPoints().stream())
        .filter(p -> matches(p, labels))
        .mapToLong(LongPointData::getValue)
        .findFirst()
        .orElse(0L);
  }

  /**
   * @return the value of the double-gauge series {@code name} for the first point matching {@code
   *     labels}, or {@code 0} if none.
   */
  public double doubleGauge(String name, Map<String, String> labels) {
    return metric(name)
        .filter(m -> m.getType() == io.opentelemetry.sdk.metrics.data.MetricDataType.DOUBLE_GAUGE)
        .stream()
        .flatMap(m -> m.getDoubleGaugeData().getPoints().stream())
        .filter(p -> matches(p, labels))
        .mapToDouble(DoublePointData::getValue)
        .findFirst()
        .orElse(0d);
  }

  /**
   * @return the number of observations recorded to the histogram series {@code name} across every
   *     point matching {@code labels}.
   */
  public long histogramCount(String name, Map<String, String> labels) {
    return histogramPoints(name, labels).mapToLong(HistogramPointData::getCount).sum();
  }

  /**
   * @return the summed observations recorded to the histogram series {@code name} across every
   *     point matching {@code labels}.
   */
  public double histogramSum(String name, Map<String, String> labels) {
    return histogramPoints(name, labels).mapToDouble(HistogramPointData::getSum).sum();
  }

  /** Returns the explicit bucket boundaries for the first matching histogram point. */
  public List<Double> histogramBoundaries(String name, Map<String, String> labels) {
    return histogramPoints(name, labels)
        .findFirst()
        .map(HistogramPointData::getBoundaries)
        .orElse(List.of());
  }

  private java.util.stream.Stream<HistogramPointData> histogramPoints(
      String name, Map<String, String> labels) {
    return metric(name)
        .filter(m -> m.getType() == io.opentelemetry.sdk.metrics.data.MetricDataType.HISTOGRAM)
        .stream()
        .flatMap(m -> m.getHistogramData().getPoints().stream())
        .filter(p -> matches(p, labels));
  }

  private java.util.Optional<MetricData> metric(String name) {
    return collect().stream().filter(m -> m.getName().equals(name)).findFirst();
  }

  private static boolean matches(PointData point, Map<String, String> labels) {
    return labels.entrySet().stream()
        .allMatch(
            e ->
                e.getValue().equals(point.getAttributes().get(AttributeKey.stringKey(e.getKey()))));
  }

  @Override
  public void close() {
    OpenTelemetryMetrics.setMeterProvider(null);
    provider.close();
  }

  /** Minimal in-memory {@link MetricReader}; mirrors the one used by {@code OTelTestObserver}. */
  private static final class InMemoryReader implements MetricReader {
    @Nullable private CollectionRegistration registration;

    @Override
    public void register(CollectionRegistration registration) {
      this.registration = registration;
    }

    Collection<MetricData> collectAllMetrics() {
      if (registration == null) {
        throw new IllegalStateException("InMemoryReader not registered");
      }
      return registration.collectAllMetrics();
    }

    @Override
    public CompletableResultCode forceFlush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
      return AggregationTemporality.CUMULATIVE;
    }
  }
}
