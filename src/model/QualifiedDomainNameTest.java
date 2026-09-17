package co.atoms.splitter.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class QualifiedDomainNameTest {
  @Test
  public void testParse() {
    QualifiedDomainName domain = QualifiedDomainName.parse("tenant/service/domain");
    assertEquals(domain.service(), QualifiedServiceName.parse("tenant/service"));
    assertEquals(domain.name(), "domain");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "tenant/service/domain/extra",
        "tenant/service",
        "/service/domain",
        "tenant//domain",
        "//",
        "tenant/service/",
        "domain",
      })
  public void testParseFail(String name) {
    try {
      QualifiedDomainName.parse(name);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Invalid qualified domain name: " + name);
    }
  }
}
