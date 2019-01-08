package java.lang;

import java.util.function.Function;
import java.util.stream.Stream;

import sun.reflect.CallerSensitive;

/**
 * Stub for the Java 9 compatibility when compiled with JDK 8.
 */
public class StackWalker {

	public static StackWalker getInstance() {
		return null;
	}

	@CallerSensitive
	public <T> T walk(Function<? super Stream<StackFrame>, ? extends T> function) {
		return null;
	}

	public interface StackFrame {
		String getClassName();

		String getMethodName();

		boolean isNativeMethod();
	}

}
