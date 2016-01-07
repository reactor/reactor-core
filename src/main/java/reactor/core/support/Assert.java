/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.support;

/**
 * Assertion utility class that assists in validating arguments. Useful for identifying programmer errors early and
 * clearly at runtime.
 * <p>
 * <p>For example, if the contract of a public method states it does not allow {@code null} arguments, Assert can be
 * used to validate that contract. Doing this clearly indicates a contract violation when it occurs and protects the
 * class's invariants.
 * <p>
 * <p>Typically used to validate method arguments rather than configuration properties, to check for cases that are
 * usually programmer errors rather than configuration errors. In contrast to config initialization code, there is
 * usally no point in falling back to defaults in such methods.
 * <p>
 * <p>This class is similar to JUnit's assertion library. If an argument value is deemed invalid, an {@link
 * IllegalArgumentException} is thrown (typically). For example:
 * <p>
 * <pre class="code"> Assert.notNull(clazz, "The class must not be null"); Assert.isTrue(i > 0, "The value must be
 * greater than zero");</pre>
 * <p>
 * Mainly for internal use within the framework; consider Jakarta's Commons Lang >= 2.0 for a more comprehensive suite
 * of assertion utilities.
 *
 * @author Keith Donald
 * @author Juergen Hoeller
 * @author Colin Sampaleanu
 * @author Rob Harrop
 * @since 1.1.2
 */
@SuppressWarnings({"rawtypes"})
public abstract class Assert {

	/**
	 * Assert a boolean expression, throwing {@code IllegalArgumentException} if the test result is {@code false}. <pre
	 * class="code">Assert.isTrue(i &gt; 0, "The value must be greater than zero");</pre>
	 *
	 * @param expression a boolean expression
	 * @param message    the error message to use if the assertion fails
	 * @throws IllegalArgumentException if expression is {@code false}
	 */
	public static void isTrue(boolean expression, String message) {
		if (!expression) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Assert a boolean expression, throwing {@code IllegalArgumentException} if the test result is {@code false}. <pre
	 * class="code">Assert.isTrue(i &gt; 0);</pre>
	 *
	 * @param expression a boolean expression
	 * @throws IllegalArgumentException if expression is {@code false}
	 */
	public static void isTrue(boolean expression) {
		isTrue(expression, "[Assertion failed] - this expression must be true");
	}

	/**
	 * Assert that an object is {@code null} . <pre class="code">Assert.isNull(value, "The value must be null");</pre>
	 *
	 * @param object  the object to check
	 * @param message the error message to use if the assertion fails
	 * @throws IllegalArgumentException if the object is not {@code null}
	 */
	public static void isNull(Object object, String message) {
		if (object != null) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Assert that an object is not {@code null} . <pre class="code">Assert.notNull(clazz, "The class must not be
	 * null");</pre>
	 *
	 * @param object  the object to check
	 * @param message the error message to use if the assertion fails
	 * @throws IllegalArgumentException if the object is {@code null}
	 */
	public static void notNull(Object object, String message) {
		if (object == null) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Assert that an object is not {@code null} . <pre class="code">Assert.notNull(clazz);</pre>
	 *
	 * @param object the object to check
	 * @throws IllegalArgumentException if the object is {@code null}
	 */
	public static void notNull(Object object) {
		notNull(object, "[Assertion failed] - this argument is required; it must not be null");
	}


	/**
	 * Assert that an array has elements; that is, it must not be {@code null} and must have at least one element. <pre
	 * class="code">Assert.notEmpty(array, "The array must have elements");</pre>
	 *
	 * @param array   the array to check
	 * @param message the error message to use if the assertion fails
	 * @throws IllegalArgumentException if the object array is {@code null} or has no elements
	 */
	public static void notEmpty(Object[] array, String message) {
		if (array == null || array.length == 0) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Assert that the provided object is an instance of the provided class. <pre class="code">Assert.instanceOf(Foo
	 * .class,
	 * foo);</pre>
	 *
	 * @param type    the type to check against
	 * @param obj     the object to check
	 * @param message a message which will be prepended to the message produced by the function itself, and which
	 *                   may be
	 *                used to provide context. It should normally end in a ": " or ". " so that the function generate
	 *                message looks ok when prepended to it.
	 * @throws IllegalArgumentException if the object is not an instance of clazz
	 * @see Class#isInstance
	 */
	public static void isInstanceOf(Class<?> type, Object obj, String message) {
		notNull(type, "Type to check against must not be null");
		if (!type.isInstance(obj)) {
			throw new IllegalArgumentException(
			  (message != null && message.length() > 0 ? message + " " : "") +
				"Object of class [" + (obj != null ? obj.getClass().getName() : "null") +
				"] must be an instance of " + type);
		}
	}

	/**
	 * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}. <pre class="code">Assert.isAssignable
	 * (Number.class,
	 * myClass);</pre>
	 *
	 * @param superType the super type to check against
	 * @param subType   the sub type to check
	 * @param message   a message which will be prepended to the message produced by the function itself, and which
	 *                     may be
	 *                  used to provide context. It should normally end in a ": " or ". " so that the function generate
	 *                  message looks ok when prepended to it.
	 * @throws IllegalArgumentException if the classes are not assignable
	 */
	public static void isAssignable(Class<?> superType, Class<?> subType, String message) {
		notNull(superType, "Type to check against must not be null");
		if (subType == null || !superType.isAssignableFrom(subType)) {
			throw new IllegalArgumentException(message + subType + " is not assignable to " + superType);
		}
	}


	/**
	 * Assert a boolean expression, throwing {@code IllegalStateException} if the test result is {@code false}. Call
	 * isTrue
	 *
	 * if you wish to throw IllegalArgumentException on an assertion failure. <pre class="code">Assert.state(id ==
	 * null,
	 * "The id property must not already be initialized");</pre>
	 *
	 * @param expression a boolean expression
	 * @param message    the error message to use if the assertion fails
	 * @throws IllegalStateException if expression is {@code false}
	 */
	public static void state(boolean expression, String message) {
		if (!expression) {
			throw new IllegalStateException(message);
		}
	}

	/**
	 * Assert a boolean expression, throwing {@link IllegalStateException} if the test result is {@code false}. <p>Call
	 * {@link #isTrue(boolean)} if you wish to throw {@link IllegalArgumentException} on an assertion failure. <pre
	 * class="code">Assert.state(id == null);</pre>
	 *
	 * @param expression a boolean expression
	 * @throws IllegalStateException if the supplied expression is {@code false}
	 */
	public static void state(boolean expression) {
		state(expression, "[Assertion failed] - this state invariant must be true");
	}

}
