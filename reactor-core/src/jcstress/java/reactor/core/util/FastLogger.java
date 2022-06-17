/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import reactor.util.Logger;

/**
 * Implementation of {@link Logger} which is based on the {@link ThreadLocal} based
 * queue which collects all the events on the per-thread basis.
 * </br>
 * Such logger is designed to have all events stored during the stress-test run and
 * then sorted and printed out once all the Threads completed execution (inside the
 * {@link org.openjdk.jcstress.annotations.Arbiter} annotated method.
 * </br>
 * Note, this implementation only supports trace-level logs and ignores all others, it
 * is intended to be used by {@link reactor.core.publisher.StateLogger}.
 */
public class FastLogger implements Logger {

	final Map<Thread, List<String>> queues = new ConcurrentHashMap<>();

	final ThreadLocal<List<String>> logsQueueLocal = ThreadLocal.withInitial(() -> {
		final ArrayList<String> logs = new ArrayList<>(100);
		queues.put(Thread.currentThread(), logs);
		return logs;
	});

	private final String name;

	public FastLogger(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return queues.values()
		             .stream()
		             .flatMap(List::stream)
					 .sorted(Comparator.comparingLong(s -> {
						 Pattern pattern = Pattern.compile("\\[(.*?)]");
						 Matcher matcher = pattern.matcher(s);
						 matcher.find();
						 return Long.parseLong(matcher.group(1));
					 }))
		             .collect(Collectors.joining("\n"));
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public boolean isTraceEnabled() {
		return true;
	}

	@Override
	public void trace(String msg) {
		logsQueueLocal.get()
		              .add(String.format("[%s] %s", System.nanoTime(), msg));
	}

	@Override
	public void trace(String format, Object... arguments) {
		trace(String.format(format, arguments));
	}

	@Override
	public void trace(String msg, Throwable t) {
		trace(String.format("%s, %s", msg, Arrays.toString(t.getStackTrace())));
	}

	@Override
	public boolean isDebugEnabled() {
		return false;
	}

	@Override
	public void debug(String msg) {

	}

	@Override
	public void debug(String format, Object... arguments) {

	}

	@Override
	public void debug(String msg, Throwable t) {

	}

	@Override
	public boolean isInfoEnabled() {
		return false;
	}

	@Override
	public void info(String msg) {

	}

	@Override
	public void info(String format, Object... arguments) {

	}

	@Override
	public void info(String msg, Throwable t) {

	}

	@Override
	public boolean isWarnEnabled() {
		return false;
	}

	@Override
	public void warn(String msg) {

	}

	@Override
	public void warn(String format, Object... arguments) {

	}

	@Override
	public void warn(String msg, Throwable t) {

	}

	@Override
	public boolean isErrorEnabled() {
		return false;
	}

	@Override
	public void error(String msg) {

	}

	@Override
	public void error(String format, Object... arguments) {

	}

	@Override
	public void error(String msg, Throwable t) {

	}
}
