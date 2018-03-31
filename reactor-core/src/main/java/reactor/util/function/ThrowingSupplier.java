package reactor.util.function;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {

    T supplier() throws E;
}
