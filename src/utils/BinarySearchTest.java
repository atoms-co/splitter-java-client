package co.atoms.splitter.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class BinarySearchTest {
  @Test
  public void testBinarySearch() {
    var list = List.of(1, 2, 3, 4, 5);
    for (int i = 1; i <= 5; ++i) {
      final var p = i;
      assertEquals(i - 1, BinarySearch.binarySearch(list, x -> x - p));
    }
  }

  @Test
  public void testBinarySearchMissing() {
    var list = List.of(1, 2, 4, 5, 7);
    assertEquals(-3, BinarySearch.binarySearch(list, x -> x - 3));
    assertEquals(-1, BinarySearch.binarySearch(list, x -> x));
    assertEquals(-5, BinarySearch.binarySearch(list, x -> x - 6));
  }

  @Test
  public void testBinarySearchEmpty() {
    var list = List.<Integer>of();
    assertEquals(-1, BinarySearch.binarySearch(list, x -> x));
  }

  @Test
  public void testBinarySearchSingle() {
    var list = List.<Integer>of(1);
    assertEquals(-1, BinarySearch.binarySearch(list, x -> x));
    assertEquals(0, BinarySearch.binarySearch(list, x -> x - 1));
    assertEquals(-2, BinarySearch.binarySearch(list, x -> x - 2));
  }
}
