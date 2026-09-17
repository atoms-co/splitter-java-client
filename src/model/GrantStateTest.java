package co.atoms.splitter.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class GrantStateTest {

  @Test
  public void testCanAdvanceTo() {
    var states =
        List.of(
            GrantState.ALLOCATED,
            GrantState.ALLOCATED_LOADED,
            GrantState.ACTIVE,
            GrantState.REVOKED,
            GrantState.REVOKED_UNLOADED);

    for (var from : states) {
      var canAdvance = false;
      for (var to : states) {
        assertEquals(from.canAdvanceTo(to), canAdvance);
        if (from == to) {
          canAdvance = true;
        }
      }
    }
  }
}
