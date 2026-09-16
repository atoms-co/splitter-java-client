package co.atoms.splitter.consumer.processor;

import co.atoms.splitter.model.GrantState;
import co.atoms.splitter.model.QualifiedDomainKey;
import java.util.Optional;

public interface GrantManager<T> {
  /**
   * Lookup returns value, if any, for the given key, with the constraint that the state is the
   * first present in the given list. If none are provided, lookup implicitly uses the default
   * notion of ownership under possible transitional states: [Active, Revoked, Loaded, Unloaded].
   */
  Optional<T> lookup(QualifiedDomainKey key, GrantState... states);
}
