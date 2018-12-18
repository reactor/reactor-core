package sun.misc;

/**
 * Stub for the Java 8 compatibility when compiled with JDK 9+.
 */
public interface JavaLangAccess {

	int getStackTraceDepth(Throwable e);

	StackTraceElement getStackTraceElement(Throwable e, int depth);
}
