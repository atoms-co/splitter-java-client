package co.atoms.splitter.testing;

import co.atoms.splitter.model.DomainType;
import co.atoms.splitter.model.Grant;
import co.atoms.splitter.model.Instance;
import co.atoms.splitter.model.Location;
import co.atoms.splitter.model.QualifiedDomainKey;
import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Fixtures {

  public static final Instant TS = Instant.parse("2021-01-01T11:22:33Z");
  public static final Instant EXP = TS.plusSeconds(1);
  public static final Instance INSTANCE =
      Instance.create("instance-id", Location.LOCAL, TS, "instance-name", "instance-endpoint");

  public static final QualifiedDomainName DOMAIN1 =
      QualifiedDomainName.parse("tenant/service/domain1");
  public static final QualifiedDomainName DOMAIN2 =
      QualifiedDomainName.parse("tenant/service2/domain2");
  public static final QualifiedDomainName DOMAIN3 =
      QualifiedDomainName.parse("tenant/service2/domain3");

  public static final Shard SHARD_D1_UNIT_0e = newUnitShard(DOMAIN1, "0", "e");
  public static final Shard SHARD_D2_GLOBAL_15 = newGlobalShard(DOMAIN2, "1", "5");
  public static final Shard SHARD_D2_GLOBAL_5c = newGlobalShard(DOMAIN2, "5", "c");
  public static final Shard SHARD_D2_GLOBAL_cf = newGlobalShard(DOMAIN2, "c", "f");
  public static final Shard SHARD_D3_REGIONAL_r1_1a =
      newShard(DOMAIN3, DomainType.REGIONAL, "region1", "1", "a");
  public static final Shard SHARD_D3_REGIONAL_r1_af =
      newShard(DOMAIN3, DomainType.REGIONAL, "region1", "a", "f");
  public static final Shard SHARD_D3_REGIONAL_r2_0a =
      newShard(DOMAIN3, DomainType.REGIONAL, "region2", "0", "a");
  public static final Shard SHARD_D3_REGIONAL_r2_af =
      newShard(DOMAIN3, DomainType.REGIONAL, "region2", "a", "f");

  public static QualifiedDomainKey newKey(QualifiedDomainName domain, String key) {
    return QualifiedDomainKey.create(domain, pad(key));
  }

  public static QualifiedDomainKey newKey(QualifiedDomainName domain, String region, String key) {
    return QualifiedDomainKey.create(domain, region, pad(key));
  }

  public static Shard newShard(
      QualifiedDomainName domain, DomainType domainType, String region, String from, String to) {
    return Shard.create(domain, domainType, region, pad(from), pad(to));
  }

  public static Shard newUnitShard(QualifiedDomainName domain, String from, String to) {
    return Shard.create(domain, DomainType.UNIT, "", pad(from), pad(to));
  }

  public static Shard newGlobalShard(QualifiedDomainName domain, String from, String to) {
    return Shard.create(domain, DomainType.GLOBAL, "", pad(from), pad(to));
  }

  /** Create a UUID with the given prefix and zeroed suffix. */
  public static UUID pad(String prefix) {
    var zero = new UUID(0, 0);
    return UUID.fromString(
        String.format("%s%s", prefix, zero.toString().substring(prefix.length())));
  }

  public static co.atoms.lib.net.location.proto.Instance newLocationInstance(String id) {
    return co.atoms.lib.net.location.proto.Instance.newBuilder().setId(id).build();
  }

  public static List<Grant> sorted(Stream<Grant> grants) {
    return grants.sorted(Comparator.comparing(Grant::id)).collect(Collectors.toList());
  }

  public static Instance newInstance(String name) {
    return Instance.create("instance-id", Location.LOCAL, TS, name, "instance-endpoint");
  }

  public static MutableClock createClock() {
    return MutableClock.at(ZoneId.systemDefault(), TS);
  }

  public static <T> StreamObserver<T> newStreamObserver(
      LinkedBlockingQueue<T> sent,
      LinkedBlockingQueue<Throwable> exceptions,
      LinkedBlockingQueue<Boolean> terminated) {
    return new StreamObserver<>() {
      @Override
      public void onNext(T value) {
        sent.add(value);
      }

      @Override
      public void onError(Throwable t) {
        exceptions.add(t);
      }

      @Override
      public void onCompleted() {
        terminated.add(true);
      }
    };
  }
}
