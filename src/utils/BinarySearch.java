package co.atoms.splitter.utils;

import java.util.List;

public class BinarySearch {
  private BinarySearch() {}

  /**
   * Functional interface that allows to compare an object of type T to some value.
   *
   * <p>Returns -1 if the given object is smaller than compared value, 0 if they are equal, 1 if the
   * given object is larger than compared value.
   */
  @FunctionalInterface
  public interface CompareFunc<T> {
    int compare(T t);
  }

  /**
   * Implementation of binary search algorithm that allows to perform comparison using a lambda
   * function.
   *
   * <p>Implementation from the platform library only allows to use types that implement {@link
   * java.lang.Comparable} and compare elements to a given element. That API doesn't allow to
   * perform binary search in collections sorted by an object's property using that property value.
   *
   * @return index of the found element, if it is contained in the list; otherwise, returns a
   *     negative value that can be used to determine the insertion point: (-(insertion point) - 1).
   */
  public static <T> int binarySearch(List<T> list, CompareFunc<T> fn) {
    int low = 0;
    int high = list.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      T midVal = list.get(mid);
      int cmp = fn.compare(midVal);

      if (cmp < 0) low = mid + 1;
      else if (cmp > 0) high = mid - 1;
      else return mid;
    }
    return -(low + 1);
  }
}
