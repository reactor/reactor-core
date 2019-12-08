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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.logging.Level;

import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.Fuseable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A logging interceptor that intercepts all reactive calls and trace them.
 * The logging level can be tuned using {@link Level}, but only FINEST, FINE, INFO,
 * WARNING and SEVERE are taken into account.
 *
 * @author Stephane Maldini
 */
final class SignalLogger<IN> implements SignalPeek<IN> {

	final static int CONTEXT_PARENT    = 0b0100000000;
	final static int SUBSCRIBE         = 0b0010000000;
	final static int ON_SUBSCRIBE      = 0b0001000000;
	final static int ON_NEXT           = 0b0000100000;
	final static int ON_ERROR          = 0b0000010000;
	final static int ON_COMPLETE       = 0b0000001000;
	final static int REQUEST           = 0b0000000100;
	final static int CANCEL            = 0b0000000010;
	final static int AFTER_TERMINATE   = 0b0000000001;
	final static int ALL               =
			CONTEXT_PARENT | CANCEL | ON_COMPLETE | ON_ERROR | REQUEST | ON_SUBSCRIBE | ON_NEXT | SUBSCRIBE;

	final static AtomicLong IDS = new AtomicLong(1);

	final CorePublisher<IN> source;

	final Logger  log;
	final boolean fuseable;
	final int     options;
	final Level   level;
	final String  operatorLine;
	final long    id;

	static final String LOG_TEMPLATE          = "{}({})";
	static final String LOG_TEMPLATE_FUSEABLE = "| {}({})";

	SignalLogger(CorePublisher<IN> source,
			@Nullable String category,
			Level level,
			boolean correlateStack,
			SignalType... options) {
		this(source, category, level, correlateStack, Loggers::getLogger, options);
	}

	SignalLogger(CorePublisher<IN> source,
			@Nullable String category,
			Level level,
			boolean correlateStack,
			Function<String, Logger> loggerSupplier,
			@Nullable SignalType... options) {

		this.source = Objects.requireNonNull(source, "source");
		this.id = IDS.getAndIncrement();
		this.fuseable = source instanceof Fuseable;

		if (correlateStack) {
			operatorLine = Traces.extractOperatorAssemblyInformation(Traces.callSiteSupplierFactory.get().get());
		}
		else {
			operatorLine = null;
		}

		boolean generated =
				category == null || category.isEmpty() || category.endsWith(".");

		category = generated && category == null ? "reactor." : category;
		if (generated) {
			if (source instanceof Mono) {
				category += "Mono." + source.getClass()
				                            .getSimpleName()
				                            .replace("Mono", "");
			}
			else if (source instanceof ParallelFlux) {
				category += "Parallel." + source.getClass()
				                                .getSimpleName()
				                                .replace("Parallel", "");
			}
			else {
				category += "Flux." + source.getClass()
				                            .getSimpleName()
				                            .replace("Flux", "");
			}
			category += "." + id;
		}

		this.log = loggerSupplier.apply(category);

		this.level = level;
		if (options == null || options.length == 0) {
			this.options = ALL;
		}
		else {
			int opts = 0;
			for (SignalType option : options) {
				if (option == SignalType.CANCEL) {
					opts |= CANCEL;
				}
				else if (option == SignalType.CURRENT_CONTEXT) {
					opts |= CONTEXT_PARENT;
				}
				else if (option == SignalType.ON_SUBSCRIBE) {
					opts |= ON_SUBSCRIBE;
				}
				else if (option == SignalType.REQUEST) {
					opts |= REQUEST;
				}
				else if (option == SignalType.ON_NEXT) {
					opts |= ON_NEXT;
				}
				else if (option == SignalType.ON_ERROR) {
					opts |= ON_ERROR;
				}
				else if (option == SignalType.ON_COMPLETE) {
					opts |= ON_COMPLETE;
				}
				else if (option == SignalType.SUBSCRIBE) {
					opts |= SUBSCRIBE;
				}
				else if (option == SignalType.AFTER_TERMINATE) {
					opts |= AFTER_TERMINATE;
				}
			}
			this.options = opts;
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;

		return null;
	}

	/**
	 * Structured logging with level adaptation and operator ascii graph if required.
	 *
	 * @param signalType the type of signal being logged
	 * @param signalValue the value for the signal (use empty string if not required)
	 */
	void log(SignalType signalType, Object signalValue) {
		String line = fuseable ? LOG_TEMPLATE_FUSEABLE : LOG_TEMPLATE;
		if (operatorLine != null) {
			line = line + " " + operatorLine;
		}
		if (level == Level.FINEST) {
			log.trace(line, signalType, signalValue);
		}
		else if (level == Level.FINE) {
			log.debug(line, signalType, signalValue);
		}
		else if (level == Level.INFO) {
			log.info(line, signalType, signalValue);
		}
		else if (level == Level.WARNING) {
			log.warn(line, signalType, signalValue);
		}
		else if (level == Level.SEVERE) {
			log.error(line, signalType, signalValue);
		}
	}

	/**
	 * Structured logging with level adaptation and operator ascii graph if required +
	 * protection against loggers that detect objects like {@link Fuseable.QueueSubscription}
	 * as {@link java.util.Collection} and attempt to use their iterator for logging.
	 *
	 * @see #log
	 */
	void safeLog(SignalType signalType, Object signalValue) {
		if (signalValue instanceof Fuseable.QueueSubscription) {
			signalValue = String.valueOf(signalValue);
			if (log.isDebugEnabled()) {
				log.debug("A Fuseable Subscription has been passed to the logging framework, this is generally a sign of a misplaced log(), " +
						"eg. 'window(2).log()' instead of 'window(2).flatMap(w -> w.log())'");
			}
		}
		try {
			log(signalType, signalValue);
		}
		catch (UnsupportedOperationException uoe) {
			log(signalType, String.valueOf(signalValue));
			if (log.isDebugEnabled()) {
				log.debug("UnsupportedOperationException has been raised by the logging framework, does your log() placement make sense? " +
						"eg. 'window(2).log()' instead of 'window(2).flatMap(w -> w.log())'", uoe);
			}
		}
	}


	static String subscriptionAsString(@Nullable Subscription s) {
		if (s == null) {
			return "null subscription";
		}
		StringBuilder asString = new StringBuilder();
		if (s instanceof Fuseable.SynchronousSubscription) {
			asString.append("[Synchronous Fuseable] ");
		}
		else if (s instanceof Fuseable.QueueSubscription) {
			asString.append("[Fuseable] ");
		}

		Class<? extends Subscription> clazz = s.getClass();
		String name = clazz.getCanonicalName();
		if (name == null) {
			name = clazz.getName();
		}
		name = name.replaceFirst(clazz.getPackage()
		                              .getName() + ".", "");
		asString.append(name);

		return asString.toString();
	}

	@Override
	@Nullable
	public Consumer<? super Subscription> onSubscribeCall() {
		if ((options & ON_SUBSCRIBE) == ON_SUBSCRIBE && (level != Level.INFO || log.isInfoEnabled())) {
			return s -> log(SignalType.ON_SUBSCRIBE, subscriptionAsString(s));
		}
		return null;
	}

	@Nullable
	@Override
	public Consumer<? super Context> onCurrentContextCall() {
		if ((options & CONTEXT_PARENT) == CONTEXT_PARENT && (level != Level.INFO || log
				.isInfoEnabled())) {
			return c -> log(SignalType.ON_CONTEXT, c);
		}
		return null;
	}

	@Override
	@Nullable
	public Consumer<? super IN> onNextCall() {
		if ((options & ON_NEXT) == ON_NEXT && (level != Level.INFO || log.isInfoEnabled())) {
			return d -> safeLog(SignalType.ON_NEXT, d);
		}
		return null;
	}

	@Override
	@Nullable
	public Consumer<? super Throwable> onErrorCall() {
		boolean shouldLogAsDebug = level == Level.FINE && log.isDebugEnabled();
		boolean shouldLogAsTrace = level == Level.FINEST && log.isTraceEnabled();
		boolean shouldLogAsError = level != Level.FINE && level != Level.FINEST && log.isErrorEnabled();
		if ((options & ON_ERROR) == ON_ERROR && (shouldLogAsError || shouldLogAsDebug ||
				shouldLogAsTrace)) {
			String line = fuseable ? LOG_TEMPLATE_FUSEABLE : LOG_TEMPLATE;
			if (operatorLine != null) {
				line = line + " " + operatorLine;
			}
			String s = line;
			if (shouldLogAsTrace) {
				return e -> {
					log.trace(s, SignalType.ON_ERROR, e, source);
					log.trace("", e);
				};
			}
			else if (shouldLogAsDebug) {
				return e -> {
					log.debug(s, SignalType.ON_ERROR, e, source);
					log.debug("", e);
				};
			}
			else { //shouldLogAsError
				return e -> {
					log.error(s, SignalType.ON_ERROR, e, source);
					log.error("", e);
				};
			}
		}
		return null;
	}

	@Override
	@Nullable
	public Runnable onCompleteCall() {
		if ((options & ON_COMPLETE) == ON_COMPLETE && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.ON_COMPLETE, "");
		}
		return null;
	}

	@Override
	@Nullable
	public Runnable onAfterTerminateCall() {
		if ((options & AFTER_TERMINATE) == AFTER_TERMINATE && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.AFTER_TERMINATE, "");
		}
		return null;
	}

	@Override
	@Nullable
	public LongConsumer onRequestCall() {
		if ((options & REQUEST) == REQUEST && (level != Level.INFO || log.isInfoEnabled())) {
			return n -> log(SignalType.REQUEST,
					Long.MAX_VALUE == n ? "unbounded" : n);
		}
		return null;
	}

	@Override
	@Nullable
	public Runnable onCancelCall() {
		if ((options & CANCEL) == CANCEL && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.CANCEL, "");
		}
		return null;
	}

	@Override
	public String toString() {
		return "/loggers/" + log.getName() + "/" + id;
	}

}
