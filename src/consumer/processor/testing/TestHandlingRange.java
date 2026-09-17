package co.atoms.splitter.consumer.processor.testing;

import co.atoms.splitter.consumer.processor.HandlingRange;
import co.atoms.splitter.model.QualifiedDomainKey;

public class TestHandlingRange extends HandlingRange<QualifiedDomainKey, QualifiedDomainKey> {
  @Override
  public QualifiedDomainKey handle(QualifiedDomainKey request) {
    return request;
  }
}
