/*
 * Copyright (c) 2018-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;

import org.jspecify.annotations.Nullable;
import reactor.util.Logger;

/**
 * A {@link Logger} that writes to {@link ByteArrayOutputStream} and allows retrieval of
 * the logs via {@link #getErrContent()} and {@link #getOutContent()}. Both buffers can
 * be cleared with {@link #reset()}.
 *
 * @author Simon Baslé
 */
public class TestLogger implements Logger {

	private final ByteArrayOutputStream errContent;
	private final ByteArrayOutputStream logContent;
	private final PrintStream err;
	private final PrintStream log;
	private final boolean logCurrentThreadName;

	public TestLogger() {
		this.logContent = new ByteArrayOutputStream();
		this.log = new PrintStream(logContent);
		this.errContent = new ByteArrayOutputStream();
		this.err = new PrintStream(errContent);
		this.logCurrentThreadName = true;
	}

	public TestLogger(boolean logCurrentThreadName){
		this.logContent = new ByteArrayOutputStream();
		this.log = new PrintStream(logContent);
		this.errContent = new ByteArrayOutputStream();
		this.err = new PrintStream(errContent);
		this.logCurrentThreadName = logCurrentThreadName;
	}

	@Override
	public String getName() {
		return "TestLogger";
	}

	public String getErrContent() {
		return errContent.toString();
	}

	public String getOutContent() {
		return logContent.toString();
	}

	public void reset() {
		this.errContent.reset();
		this.logContent.reset();
	}

	private @Nullable String format(@Nullable String from, @Nullable Object @Nullable... arguments) {
		if(from != null) {
			String computed = from;
			if (arguments != null && arguments.length != 0) {
				for (Object argument : arguments) {
					computed = computed.replaceFirst("\\{\\}", Matcher.quoteReplacement(String.valueOf(argument)));
				}
			}
			return computed;
		}
		return null;
	}

	@Override
	public boolean isTraceEnabled() {
		return true;
	}

	@Override
	public synchronized void trace(String msg) {
		this.log.format(logContent("TRACE", msg));
	}

	@Override
	public synchronized void trace(String format, @Nullable Object @Nullable... arguments) {
		this.log.format(logContent("TRACE", format(format, arguments)));
	}
	@Override
	public synchronized void trace(String msg, Throwable t) {
		this.log.format(logContent("TRACE", String.format("%s - %s", msg, t)));
		t.printStackTrace(this.err);
	}

	@Override
	public boolean isDebugEnabled() {
		return true;
	}

	@Override
	public synchronized void debug(String msg) {
		this.log.format(logContent("DEBUG", msg));
	}

	@Override
	public synchronized void debug(String format, @Nullable Object @Nullable... arguments) {
		this.log.format(logContent("DEBUG", format(format, arguments)));
	}

	@Override
	public synchronized void debug(String msg, Throwable t) {
		this.log.format(logContent("DEBUG", String.format("%s - %s", msg, t)));
		t.printStackTrace(this.err);
	}

	@Override
	public boolean isInfoEnabled() {
		return true;
	}

	@Override
	public synchronized void info(String msg) {
		this.log.format(logContent(" INFO", msg));
	}

	@Override
	public synchronized void info(String format, @Nullable Object @Nullable... arguments) {
		this.log.format(logContent(" INFO", format(format, arguments)));
	}

	@Override
	public synchronized void info(String msg, Throwable t) {
		this.log.format(logContent(" INFO", String.format("%s - %s", msg, t)));
		t.printStackTrace(this.err);
	}

	@Override
	public boolean isWarnEnabled() {
		return true;
	}

	@Override
	public synchronized void warn(String msg) {
		this.err.format(logContent(" WARN", msg));
	}

	@Override
	public synchronized void warn(String format, @Nullable Object @Nullable... arguments) {
		this.err.format(logContent(" WARN", format(format, arguments)));
	}

	@Override
	public synchronized void warn(String msg, Throwable t) {
		this.err.format(logContent(" WARN", String.format("%s - %s", msg, t)));
		t.printStackTrace(this.err);
	}

	@Override
	public boolean isErrorEnabled() {
		return true;
	}

	@Override
	public synchronized void error(String msg) {
		this.err.format(logContent("ERROR", msg));
	}

	@Override
	public synchronized void error(String format, @Nullable Object @Nullable... arguments) {
		this.err.format(logContent("ERROR", format(format, arguments)));
	}

	@Override
	public synchronized void error(String msg, Throwable t) {
		this.err.format(logContent("ERROR", String.format("%s - %s", msg, t)));
		t.printStackTrace(this.err);
	}

	String logContent(String logType, @Nullable String msg){
		if(logCurrentThreadName){
			return String.format("[%s] (%s) %s\n", logType,
					Thread.currentThread().getName(), msg);
		} else {
			return String.format("[%s] %s\n", logType, msg);
		}
	}

	public boolean isLogCurrentThreadName() {
		return logCurrentThreadName;
	}
}
