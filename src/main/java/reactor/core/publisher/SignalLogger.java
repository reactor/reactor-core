/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.util.Logger;
import reactor.util.Loggers;


/**
 * A logging interceptor that intercepts all reactive calls and trace them
 *
 * @author Stephane Maldini
 */
final class SignalLogger<IN> implements SignalPeek<IN> {

	final static int SUBSCRIBE       = 0b010000000;
	final static int ON_SUBSCRIBE    = 0b001000000;
	final static int ON_NEXT         = 0b000100000;
	final static int ON_ERROR        = 0b000010000;
	final static int ON_COMPLETE     = 0b000001000;
	final static int REQUEST         = 0b000000100;
	final static int CANCEL          = 0b000000010;
	final static int AFTER_TERMINATE = 0b000000001;
	final static int ALL             =
			CANCEL | ON_COMPLETE | ON_ERROR | REQUEST | ON_SUBSCRIBE | ON_NEXT | SUBSCRIBE;

	final static AtomicLong IDS = new AtomicLong(1);

	final Publisher<IN> source;

	final Logger  log;
	final boolean fuseable;
	final int     options;
	final Level   level;
	final String  operatorLine;
	final long   id;

	static final String LOG_TEMPLATE = "{}({})";
	static final String LOG_TEMPLATE_FUSEABLE = "| {}({})";

	public SignalLogger(Publisher<IN> source,
			String category,
			Level level,
			boolean correlateStack,
			SignalType... options) {
		this(source, category, level, correlateStack, Loggers::getLogger, options);
	}

	public SignalLogger(Publisher<IN> source,
			String category,
			Level level,
			boolean correlateStack,
			Function<String, Logger> loggerSupplier,
			SignalType... options) {

		this.source = Objects.requireNonNull(source, "source");
		this.id = IDS.getAndIncrement();
		this.fuseable = source instanceof Fuseable;

		if(correlateStack){
			operatorLine = FluxOnAssembly.extract(FluxOnAssembly.takeStacktrace(null),false);
		}
		else{
			operatorLine = null;
		}

		boolean generated = category == null || category.isEmpty() || category.endsWith(".");

		category = generated && category == null ? "reactor." : category;
		if (generated) {
			if (source instanceof Mono) {
				category += "Mono." + source.getClass()
				                           .getSimpleName()
				                           .replace("Mono", "");
			}
			else if(source instanceof ParallelFlux){
				category += "Parallel." + source.getClass()
				                            .getSimpleName()
				                            .replace("Parallel", "")
				                            .replace("Unordered", "");
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
		if(options == null || options.length == 0){
			this.options = ALL;
		}
		else{
			int opts = 0;
			for(SignalType option : options){
				if(option == SignalType.CANCEL){
					opts |= CANCEL;
				}
				else if(option == SignalType.ON_SUBSCRIBE){
					opts |= ON_SUBSCRIBE;
				}
				else if(option == SignalType.REQUEST){
					opts |= REQUEST;
				}
				else if(option == SignalType.ON_NEXT){
					opts |= ON_NEXT;
				}
				else if(option == SignalType.ON_ERROR){
					opts |= ON_ERROR;
				}
				else if(option == SignalType.ON_COMPLETE){
					opts |= ON_COMPLETE;
				}
				else if(option == SignalType.SUBSCRIBE){
					opts |= SUBSCRIBE;
				}
				else if (option == SignalType.AFTER_TERMINATE) {
					opts |= AFTER_TERMINATE;
				}
			}
			this.options = opts;
		}
	}

	void log(Object... args) {
		String line = fuseable ? LOG_TEMPLATE_FUSEABLE : LOG_TEMPLATE;
		if(operatorLine != null){
			line = line + " " + operatorLine;
		}
		if (level == Level.FINEST) {
			log.trace(line, args);
		}
		else if (level == Level.FINE) {
			log.debug(line, args);
		}
		else if (level == Level.INFO) {
			log.info(line, args);
		}
		else if (level == Level.WARNING) {
			log.warn(line, args);
		}
		else if (level == Level.SEVERE) {
			log.error(line, args);
		}
	}

	@Override
	public Consumer<? super Subscription> onSubscribeCall() {
		if ((options & ON_SUBSCRIBE) == ON_SUBSCRIBE && (level != Level.INFO || log.isInfoEnabled())) {
			return s -> log(SignalType.ON_SUBSCRIBE, s == null ? null : s.toString(), source);
		}
		return null;
	}

	@Override
	public Consumer<? super IN> onNextCall() {
		if ((options & ON_NEXT) == ON_NEXT && (level != Level.INFO || log.isInfoEnabled())) {
			return d -> log(SignalType.ON_NEXT, d, source);
		}
		return null;
	}

	@Override
	public Consumer<? super Throwable> onErrorCall() {
		if ((options & ON_ERROR) == ON_ERROR && log.isErrorEnabled()) {
			String line = fuseable ? LOG_TEMPLATE_FUSEABLE : LOG_TEMPLATE;
			if(operatorLine != null){
				line = line + " " + operatorLine;
			}
			String s = line;
			return e -> {
				log.error(s, SignalType.ON_ERROR, e, source);
				log.error("", e);
			};
		}
		return null;
	}

	@Override
	public Runnable onCompleteCall() {
		if ((options & ON_COMPLETE) == ON_COMPLETE && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.ON_COMPLETE, "", source);
		}
		return null;
	}

	@Override
	public Runnable onAfterTerminateCall() {
		if ((options & AFTER_TERMINATE) == AFTER_TERMINATE && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.AFTER_TERMINATE, "", source);
		}
		return null;
	}

	@Override
	public LongConsumer onRequestCall() {
		if ((options & REQUEST) == REQUEST && (level != Level.INFO || log.isInfoEnabled())) {
			return n -> log(SignalType.REQUEST,
					Long.MAX_VALUE == n ? "unbounded" : n,
					source);
		}
		return null;
	}

	@Override
	public Runnable onCancelCall() {
		if ((options & CANCEL) == CANCEL && (level != Level.INFO || log.isInfoEnabled())) {
			return () -> log(SignalType.CANCEL, "", source);
		}
		return null;
	}

	@Override
	public String toString() {
		return "/loggers/" + log.getName() + "/" + id;
	}

	@Override
	public Publisher<? extends IN> upstream() {
		return source;
	}
}
