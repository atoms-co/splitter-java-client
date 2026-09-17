package co.atoms.splitter.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class QualifiedServiceNameTest {
  @Test
  public void testParse() {
    QualifiedServiceName service = QualifiedServiceName.parse("tenant/service");
    assertEquals(service.tenant(), "tenant");
    assertEquals(service.name(), "service");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "tenant/service/domain",
        "/service",
        "tenant/",
        "/",
        "service",
      })
  public void testParseFail(String name) {
    try {
      QualifiedServiceName.parse(name);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Invalid qualified service name: " + name);
    }
  }
}
