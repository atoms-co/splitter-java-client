package co.atoms.splitter.utils;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UUIDComparatorTest {
  static final String ZERO = "00000000-0000-0000-0000-000000000000";

  // [1111, 2222, 3333, ..., ffff]
  static final ImmutableList<UUID> UUID_LIST =
      IntStream.range(0, 16)
          .mapToObj(Integer::toHexString)
          .map(hex -> ZERO.replace("0", hex))
          .map(UUID::fromString)
          .collect(ImmutableList.toImmutableList());

  /**
   * Test shows that UUID sorts in JDK UUID's order. If this ever gets fixed, the custom comparator
   * can be removed
   */
  @Test
  void builtInOrdering() {
    assertIterableEquals(
        UUID_LIST.stream().sorted().collect(toList()),
        IntStream.of(8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7)
            .mapToObj(UUID_LIST::get)
            .collect(toList()));
  }

  /** Test shows that UUIDComparator sorts in RFC4122 order. */
  @Test
  void customOrdering() {
    assertIterableEquals(
        UUID_LIST.stream().sorted(UUIDComparator.INSTANCE).collect(toList()),
        IntStream.range(0, 16).mapToObj(UUID_LIST::get).collect(toList()));
  }

  @ParameterizedTest
  @MethodSource("compareTwoArgs")
  void compareTwo(String small, String big) {
    UUID a = UUID.fromString(small);
    UUID b = UUID.fromString(big);
    assertEquals(UUIDComparator.INSTANCE.compare(a, b), -1);
    assertEquals(UUIDComparator.INSTANCE.compare(b, a), 1);
  }

  static Stream<Arguments> compareTwoArgs() {
    return Stream.of(
        arguments("00000000-0000-0000-0000-100000000000", "00000000-0000-0000-0000-200000000000"),
        arguments("00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"),
        arguments("00000000-0000-0000-0000-000000000000", "f0000000-0000-0000-0000-000000000000"));
  }

  @ParameterizedTest
  @MethodSource("compareTwoArgs")
  void isBefore(String small, String big) {
    UUID a = UUID.fromString(small);
    UUID b = UUID.fromString(big);
    assertTrue(UUIDComparator.isBefore(a, b));
    assertFalse(UUIDComparator.isBefore(b, a));
  }

  @ParameterizedTest
  @MethodSource("compareTwoArgs")
  void isAfter(String small, String big) {
    UUID a = UUID.fromString(small);
    UUID b = UUID.fromString(big);
    assertTrue(UUIDComparator.isAfter(b, a));
    assertFalse(UUIDComparator.isAfter(a, b));
  }

  @Test
  void isBetween() {
    UUID a = UUID.fromString("20000000-0000-0000-0000-000000000000");
    UUID b = UUID.fromString("30000000-0000-0000-0000-000000000000");

    assertTrue(
        UUIDComparator.isBetween(UUID.fromString("25000000-0000-0000-0000-000000000000"), a, b));
    assertFalse(
        UUIDComparator.isBetween(UUID.fromString("15000000-0000-0000-0000-000000000000"), a, b));
    assertFalse(
        UUIDComparator.isBetween(UUID.fromString("35000000-0000-0000-0000-000000000000"), a, b));

    try {
      UUIDComparator.isBetween(UUID.fromString("35000000-0000-0000-0000-000000000000"), b, a);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
      assertEquals(e.getMessage(), "to must be after from");
    }
  }
}
