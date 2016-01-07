package reactor.fn;

/**
 * Implementations of this class perform work on the given parameter and return a result of the same type.
 *
 * @param <T> The type of the input (and ouput) to the apply operation
 *
 * @author Oleksandr Petrov
 */
public interface UnaryOperator<T> extends Function<T, T> {
}
