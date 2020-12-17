/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.util;

/**
 * Logger interface designed for internal Reactor usage.
 */
public interface Logger {

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

}
