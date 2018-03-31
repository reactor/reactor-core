package reactor.util.function;

@FunctionalInterface
public interface ThrowingConsumer<E extends Exception> {

    void consumer() throws E;
}
