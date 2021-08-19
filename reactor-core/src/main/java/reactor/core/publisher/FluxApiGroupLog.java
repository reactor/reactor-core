/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.logging.Level;

import reactor.core.Fuseable;
import reactor.util.Logger;
import reactor.util.annotation.Nullable;

/**
 * A {@link Flux} API sub-group that exposes advanced logging configurations, all based
 * on a {@link Level} provided at top level when calling {@link Flux#logAtLevel(Level)}.
 *
 * @author Simon Baslé
 */
public final class FluxApiGroupLog<T> {

	final Flux<T> source;
	final Level level;

	FluxApiGroupLog(Flux<T> source, Level level) {
		this.source = source;
		this.level = level;
	}

	/**
	 * Observe Reactive Streams signals and trace them using {@link Logger} support,
	 * filtering which signals to log according to the {@code option} (if any).
	 * Logging is done by SLF4J ifs available or {@code java.util.logging} otherwise.
	 * The logger's name is derived from the operator that is being logged.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).withOperatorName(SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * Category is mapped into logger configuration (e.g. org.springframework.reactor).
	 * If category ends with "." like "reactor.", a generated operator suffix will be added,
	 * e.g. "reactor.Flux.Map".
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public Flux<T> withOperatorName(SignalType... options) {
		return withOperatorName(false, options);
	}

	/**
	 * Observe Reactive Streams signals and trace them using {@link Logger} support,
	 * filtering which signals to log according to the {@code option} (if any).
	 * Logging is done by SLF4J ifs available or {@code java.util.logging} otherwise.
	 * The logger's name is derived from the operator that is being logged.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).withOperatorName(SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * Category is mapped into logger configuration (e.g. org.springframework.reactor).
	 * If category ends with "." like "reactor.", a generated operator suffix will be added,
	 * e.g. "reactor.Flux.Map".
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param showOperatorLine capture the current stack to display operator class/line number.
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */

	public Flux<T> withOperatorName(boolean showOperatorLine, SignalType... options) {
		return log(this.source, (String) null, this.level, showOperatorLine, options);
	}

	/**
	 * Observe Reactive Streams signals and trace them using {@link Logger} support,
	 * filtering which signals to log according to the {@code option} (if any).
	 * Logging is done by SLF4J ifs available or {@code java.util.logging} otherwise.
	 * The provided {@code category} is used as the logger's name.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).usingCategory("category", SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * Category is mapped into logger configuration (e.g. org.springframework.reactor).
	 * If category ends with "." like "reactor.", a generated operator suffix will be added,
	 * e.g. "reactor.Flux.Map".
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public Flux<T> usingCategory(String category, SignalType... options) {
		return usingCategory(category, false, options);
	}

	/**
	 * Observe Reactive Streams signals and trace them using {@link Logger} support,
	 * filtering which signals to log according to the {@code option} (if any).
	 * Logging is done by SLF4J ifs available or {@code java.util.logging} otherwise.
	 * The provided {@code category} is used as the logger's name.
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).usingCategory("category", SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * Category is mapped into logger configuration (e.g. org.springframework.reactor).
	 * If category ends with "." like "reactor.", a generated operator suffix will be added,
	 * e.g. "reactor.Flux.Map".
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param category to be mapped into logger configuration
	 * @param showOperatorLine capture the current stack to display operator class/line number.
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public Flux<T> usingCategory(String category, boolean showOperatorLine, SignalType... options) {
		return log(this.source, category, this.level, showOperatorLine, options);
	}

	/**
	 * Observe Reactive Streams signals and trace them using the provided {@link Logger} directly,
	 * filtering which signals to log according to the {@code option} (if any).
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).usingLogger(myLogger, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public Flux<T> usingLogger(Logger logger, SignalType... options) {
		return usingLogger(logger, false, options);
	}

	/**
	 * Observe Reactive Streams signals and trace them using the provided {@link Logger} directly,
	 * filtering which signals to log according to the {@code option} (if any).
	 * <p>
	 * Options allow fine grained filtering of the traced signal, for instance to only
	 * capture onNext and onError:
	 * <pre>
	 *     flux.logAtLevel(Level.INFO).usingLogger(myLogger, SignalType.ON_NEXT, SignalType.ON_ERROR)
	 * </pre>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/logForFlux.svg" alt="">
	 *
	 * @param logger the {@link Logger} to use, instead of resolving one through a category.
	 * @param showOperatorLine capture the current stack to display operator class/line number.
	 * @param options a vararg {@link SignalType} to optionally filter log messages
	 *
	 * @return a new {@link Flux} that logs signals
	 */
	public Flux<T> usingLogger(Logger logger, boolean showOperatorLine, SignalType... options) {
		return log(this.source, logger, this.level, showOperatorLine, options);
	}


	//private to operators but reusable by Flux

	static <R> Flux<R> log(Flux<R> source,
						   @Nullable String category,
						   Level level,
						   boolean showOperatorLine,
						   SignalType... options) {
		SignalLogger<R> log = new SignalLogger<>(source, category, level, showOperatorLine, options);

		if (source instanceof Fuseable) {
			return Flux.onAssembly(new FluxLogFuseable<>(source, log));
		}
		return Flux.onAssembly(new FluxLog<>(source, log));
	}

	static <R> Flux<R> log(Flux<R> source,
						   Logger logger,
						   Level level,
						   boolean showOperatorLine,
						   SignalType... options) {
		SignalLogger<R> log = new SignalLogger<>(source, "IGNORED", level,
			showOperatorLine,
			s -> logger,
			options);

		if (source instanceof Fuseable) {
			return Flux.onAssembly(new FluxLogFuseable<>(source, log));
		}
		return Flux.onAssembly(new FluxLog<>(source, log));
	}
}
