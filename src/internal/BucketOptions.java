package co.atoms.splitter.internal;

import com.google.common.collect.Comparators;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** Helper class to specify histogram bucket options */
public class BucketOptions {

  /** Type of bucket distribution to use. */
  public enum BucketDistributionType {
    /**
     * Buckets exponentially distributed between start and end. Typically used for latency metrics.
     */
    EXPONENTIAL_DISTRIBUTION,
    /** Uniformly distributed buckets between start and end. */
    UNIFORM_DISTRIBUTION,
    /** Buckets are provided by the client */
    CUSTOM_DISTRIBUTION
  }

  public static final int MAX_BUCKETS = 25; // no more than 25 buckets

  private final double startBucket;
  private final double endBucket;
  private final int bucketCount;
  private final String unit;
  private final BucketDistributionType distributionType;
  private final List<Double> customBoundaries;

  private BucketOptions(String unit, List<Double> boundaries) {
    int n = boundaries.size();
    checkState(n > 1, "number of custom boundaries must be greater than 1");
    checkState(n <= MAX_BUCKETS, "number of custom boundaries must be at most " + MAX_BUCKETS);
    checkState(
        Comparators.isInOrder(boundaries, Comparator.naturalOrder()),
        "custom boundaries must be sorted in ascending order");

    this.startBucket = -1;
    this.endBucket = -1;
    this.bucketCount = n;
    this.unit = unit;
    this.distributionType = BucketDistributionType.CUSTOM_DISTRIBUTION;
    this.customBoundaries = boundaries;
  }

  /** Creates bucket options with custom boundaries */
  public static BucketOptions of(String unit, Double... boundaries) {
    return new BucketOptions(unit, Arrays.asList(boundaries));
  }

  public static List<Double> getBucketBoundaries(BucketOptions bucketOptions) {
    int numBuckets = bucketOptions.bucketCount;
    if (numBuckets > MAX_BUCKETS) {
      numBuckets =
          MAX_BUCKETS; // set some sane bucket limit here to not explode metric cardinality.
    }

    return
        switch (bucketOptions.distributionType) {
          case UNIFORM_DISTRIBUTION ->
              getUniformBucketBoundaries(
                  bucketOptions.startBucket, bucketOptions.endBucket, numBuckets);
          case EXPONENTIAL_DISTRIBUTION ->
              getExponentialBucketBoundaries(
                  bucketOptions.startBucket, bucketOptions.endBucket, numBuckets);
          case CUSTOM_DISTRIBUTION -> bucketOptions.customBoundaries;
        };
  }

  // getBucketBoundaries from options calculates the buckets exponentially
  static List<Double> getExponentialBucketBoundaries(
      double startBucket, double endBucket, int numBuckets) {
    Double[] buckets = new Double[numBuckets];
    // calculate the exponential growth factor based on the start, end and num buckets
    double factor = Math.pow(endBucket - startBucket, 1D / (numBuckets - 1));
    // set the start & end buckets
    buckets[0] = startBucket;
    buckets[numBuckets - 1] = endBucket;
    for (int i = 1; i < numBuckets - 1; i++) {
      // Intentionally round this off as recommended by prometheus/M3DB and stats
      // for basic tracking purposes.
      buckets[i] = startBucket + (double) Math.round(Math.pow(factor, i));
      // if the previous value is >= current value, just increment by 1
      if (buckets[i - 1] >= buckets[i]) {
        buckets[i] = buckets[i - 1] + 1;
      }
    }

    // sanity check the last bucket too - can happen if the buckets are too granular
    if (buckets[numBuckets - 2] >= buckets[numBuckets - 1]) {
      buckets[numBuckets - 1] = buckets[numBuckets - 2] + 1;
    }

    return Arrays.asList(buckets);
  }

  static List<Double> getUniformBucketBoundaries(
      double startBucket, double endBucket, int numBuckets) {
    Double[] buckets = new Double[numBuckets];
    // Calculate the step between buckets uniformly.
    double step = (endBucket - startBucket) / ((double) numBuckets - 1);

    // set the start & end buckets
    buckets[0] = startBucket;
    buckets[numBuckets - 1] = endBucket;

    for (int i = 1; i < numBuckets - 1; i++) {
      // Intentionally round this off as recommended by prometheus/M3DB and stats
      // for basic tracking purposes.
      buckets[i] = startBucket + Math.round((step * i));
    }

    return Arrays.asList(buckets);
  }

  public String getUnit() {
    return this.unit;
  }
}
