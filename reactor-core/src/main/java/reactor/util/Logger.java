/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util;

import java.util.function.Supplier;

/**
 * Logger interface designed for internal Reactor usage.
 */
public interface Logger {

	/**
	 * A kind of {@link java.util.function.Predicate} and {@link Supplier} mix, provides two
	 * variants of a message {@link String} depending on the level of detail desired.
	 */
	@FunctionalInterface
	interface ChoiceOfMessageSupplier {

		/**
		 * Provide two possible versions of a message {@link String}, depending on the
		 * level of detail desired.
		 *
		 * @param isVerbose {@code true} for higher level of detail, {@code false} for lower level of detail
		 * @return the message {@link String} according to the passed level of detail
		 */
		String get(boolean isVerbose);
	}

	/**
	 * Return the name of this <code>Logger</code> instance.
	 * @return name of this logger instance
	 */
	String getName();

	/**
	 * Is the logger instance enabled for the TRACE level?
	 *
	 * @return True if this Logger is enabled for the TRACE level,
	 *         false otherwise.
	 */
	boolean isTraceEnabled();

	/**
	 * Log a message at the TRACE level.
	 *
	 * @param msg the message string to be logged
	 */
	void trace(String msg);

	/**
	 * Log a message at the TRACE level according to the specified format
	 * and arguments.
	 * <p/>
	 * <p>This form avoids superfluous string concatenation when the logger
	 * is disabled for the TRACE level. However, this variant incurs the hidden
	 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
	 * even if this logger is disabled for TRACE.</p>
	 *
	 * @param format    the format string
	 * @param arguments a list of 3 or more arguments
	 */
	void trace(String format, Object... arguments);

	/**
	 * Log an exception (throwable) at the TRACE level with an
	 * accompanying message.
	 *
	 * @param msg the message accompanying the exception
	 * @param t   the exception (throwable) to log
	 */
	void trace(String msg, Throwable t);

	/**
	 * Is the logger instance enabled for the DEBUG level?
	 *
	 * @return True if this Logger is enabled for the DEBUG level,
	 *         false otherwise.
	 */
	boolean isDebugEnabled();

	/**
	 * Log a message at the DEBUG level.
	 *
	 * @param msg the message string to be logged
	 */
	void debug(String msg);

	/**
	 * Log a message at the DEBUG level according to the specified format
	 * and arguments.
	 * <p/>
	 * <p>This form avoids superfluous string concatenation when the logger
	 * is disabled for the DEBUG level. However, this variant incurs the hidden
	 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
	 * even if this logger is disabled for DEBUG. </p>
	 *
	 * @param format    the format string
	 * @param arguments a list of 3 or more arguments
	 */
	void debug(String format, Object... arguments);

	/**
	 * Log an exception (throwable) at the DEBUG level with an
	 * accompanying message.
	 *
	 * @param msg the message accompanying the exception
	 * @param t   the exception (throwable) to log
	 */
	void debug(String msg, Throwable t);

	/**
	 * Is the logger instance enabled for the INFO level?
	 *
	 * @return True if this Logger is enabled for the INFO level,
	 *         false otherwise.
	 */
	boolean isInfoEnabled();

	/**
	 * Log a message at the INFO level.
	 *
	 * @param msg the message string to be logged
	 */
	void info(String msg);

	/**
	 * Log a message at the INFO level according to the specified format
	 * and arguments.
	 * <p/>
	 * <p>This form avoids superfluous string concatenation when the logger
	 * is disabled for the INFO level. However, this variant incurs the hidden
	 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
	 * even if this logger is disabled for INFO. </p>
	 *
	 * @param format    the format string
	 * @param arguments a list of 3 or more arguments
	 */
	void info(String format, Object... arguments);

	/**
	 * Log an exception (throwable) at the INFO level with an
	 * accompanying message.
	 *
	 * @param msg the message accompanying the exception
	 * @param t   the exception (throwable) to log
	 */
	void info(String msg, Throwable t);

	/**
	 * Convenience method to log a message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, INFO level is used (unless {@link #isInfoEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for INFO level
	 * @see #info(String)
	 */
	default void infoOrDebug(ChoiceOfMessageSupplier messageSupplier) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true));
		}
		else if (isInfoEnabled()) {
			info(messageSupplier.get(false));
		}
	}

	/**
	 * Convenience method to log an exception (throwable), with an accompanying
	 * message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, INFO level is used (unless {@link #isInfoEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for INFO level
	 * @param cause the {@link Throwable} the original exception to be logged
	 * @see #info(String, Throwable)
	 */
	default void infoOrDebug(ChoiceOfMessageSupplier messageSupplier, Throwable cause) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true), cause);
		}
		else if (isInfoEnabled()) {
			info(messageSupplier.get(false), cause);
		}
	}

	/**
	 * Is the logger instance enabled for the WARN level?
	 *
	 * @return True if this Logger is enabled for the WARN level,
	 *         false otherwise.
	 */
	boolean isWarnEnabled();

	/**
	 * Log a message at the WARN level.
	 *
	 * @param msg the message string to be logged
	 */
	void warn(String msg);

	/**
	 * Log a message at the WARN level according to the specified format
	 * and arguments.
	 * <p/>
	 * <p>This form avoids superfluous string concatenation when the logger
	 * is disabled for the WARN level. However, this variant incurs the hidden
	 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
	 * even if this logger is disabled for WARN. </p>
	 *
	 * @param format    the format string
	 * @param arguments a list of 3 or more arguments
	 */
	void warn(String format, Object... arguments);

	/**
	 * Log an exception (throwable) at the WARN level with an
	 * accompanying message.
	 *
	 * @param msg the message accompanying the exception
	 * @param t   the exception (throwable) to log
	 */
	void warn(String msg, Throwable t);

	/**
	 * Convenience method to log a message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, WARN level is used (unless {@link #isWarnEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for WARN level
	 * @see #warn(String)
	 */
	default void warnOrDebug(ChoiceOfMessageSupplier messageSupplier) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true));
		}
		else if (isWarnEnabled()) {
			warn(messageSupplier.get(false));
		}
	}

	/**
	 * Convenience method to log an exception (throwable), with an accompanying
	 * message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, WARN level is used (unless {@link #isWarnEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for WARN level
	 * @param cause the {@link Throwable} the original exception to be logged
	 * @see #warn(String, Throwable)
	 */
	default void warnOrDebug(ChoiceOfMessageSupplier messageSupplier, Throwable cause) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true), cause);
		}
		else if (isWarnEnabled()) {
			warn(messageSupplier.get(false), cause);
		}
	}

	/**
	 * Is the logger instance enabled for the ERROR level?
	 *
	 * @return True if this Logger is enabled for the ERROR level,
	 *         false otherwise.
	 */
	boolean isErrorEnabled();

	/**
	 * Log a message at the ERROR level.
	 *
	 * @param msg the message string to be logged
	 */
	void error(String msg);

	/**
	 * Log a message at the ERROR level according to the specified format
	 * and arguments.
	 * <p/>
	 * <p>This form avoids superfluous string concatenation when the logger
	 * is disabled for the ERROR level. However, this variant incurs the hidden
	 * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
	 * even if this logger is disabled for ERROR. </p>
	 *
	 * @param format    the format string
	 * @param arguments a list of 3 or more arguments
	 */
	void error(String format, Object... arguments);

	/**
	 * Log an exception (throwable) at the ERROR level with an
	 * accompanying message.
	 *
	 * @param msg the message accompanying the exception
	 * @param t   the exception (throwable) to log
	 */
	void error(String msg, Throwable t);

	/**
	 * Convenience method to log a message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, ERROR level is used (unless {@link #isErrorEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for ERROR level
	 * @see #error(String)
	 */
	default void errorOrDebug(ChoiceOfMessageSupplier messageSupplier) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true));
		}
		else if (isErrorEnabled()) {
			error(messageSupplier.get(false));
		}
	}

	/**
	 * Convenience method to log an exception (throwable), with an accompanying
	 * message that is different according to the log level.
	 * In priority, DEBUG level is used if {@link #isDebugEnabled()}.
	 * Otherwise, ERROR level is used (unless {@link #isErrorEnabled()} is false).
	 * <p>
	 * This can be used to log different level of details according to the active
	 * log level.
	 *
	 * @param messageSupplier the {@link ChoiceOfMessageSupplier} invoked in priority
	 * with {@code true} for the DEBUG level message, or {@code false} for ERROR level
	 * @param cause the {@link Throwable} the original exception to be logged
	 * @see #error(String, Throwable)
	 */
	default void errorOrDebug(ChoiceOfMessageSupplier messageSupplier, Throwable cause) {
		if (isDebugEnabled()) {
			debug(messageSupplier.get(true), cause);
		}
		else if (isErrorEnabled()) {
			error(messageSupplier.get(false), cause);
		}
	}

}
