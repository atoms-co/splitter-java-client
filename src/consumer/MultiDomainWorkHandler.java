package co.atoms.splitter.consumer;

import co.atoms.splitter.model.QualifiedDomainName;
import co.atoms.splitter.model.Shard;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDomainWorkHandler implements WorkHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiDomainWorkHandler.class);

  private final Map<QualifiedDomainName, WorkHandler> handlers;

  public MultiDomainWorkHandler(Map<QualifiedDomainName, WorkHandler> handlers) {
    this.handlers = Collections.unmodifiableMap(handlers);
  }

  @Override
  public void handleWork(String id, Shard shard, Ownership ownership) {
    QualifiedDomainName domain = shard.domain();
    WorkHandler handler = handlers.get(domain);
    if (handler == null) {
      LOGGER.error("No handler for domain {}", domain);
      return;
    }
    handler.handleWork(id, shard, ownership);
  }
}
