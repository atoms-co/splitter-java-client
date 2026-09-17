package co.atoms.splitter.consumer.processor.exceptions;

import co.atoms.splitter.model.QualifiedDomainKey;

public class OwnerNotFoundException extends RuntimeException {
  public OwnerNotFoundException(QualifiedDomainKey key) {
    super("No owner found for key " + key);
  }
}
