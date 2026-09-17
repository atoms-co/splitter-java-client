package co.atoms.splitter.consumer.processor;

/** Functional interface for handling requests. */
@FunctionalInterface
public interface RequestHandler<REQ, RESP> {
  RESP handle(REQ request);
}
