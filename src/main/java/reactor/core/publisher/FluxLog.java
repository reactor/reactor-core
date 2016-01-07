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

import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.Logger;

/**
 * A logging interceptor that intercepts all reactive calls and trace them
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public final class FluxLog<IN> extends Flux.FluxBarrier<IN, IN> {

	public static final int SUBSCRIBE    = 0b010000000;
	public static final int ON_SUBSCRIBE = 0b001000000;
	public static final int ON_NEXT      = 0b000100000;
	public static final int ON_ERROR     = 0b000010000;
	public static final int ON_COMPLETE  = 0b000001000;
	public static final int REQUEST      = 0b000000100;
	public static final int CANCEL       = 0b000000010;
	public static final int TERMINAL     = CANCEL | ON_COMPLETE | ON_ERROR;

	public static final int ALL = TERMINAL | REQUEST | ON_SUBSCRIBE | ON_NEXT | SUBSCRIBE;

	public enum SignalKind {request, onSubscribe, onNext, onError, onComplete, cancel, graph}

	private final Logger log;
	private final Level  level;

	private final int options;

	private long uniqueId = 1L;

	public FluxLog(Publisher<IN> source, final String category, Level level, int options) {
		super(source);
		this.log =
				category != null && !category.isEmpty() ? Logger.getLogger(category) : Logger.getLogger(FluxLog.class);
		this.options = options;
		this.level = level;
	}

	@Override
	public String getName() {
		return "/loggers/" + (log.getName()
		                         .equalsIgnoreCase(FluxLog.class.getName()) ? "default" : log.getName());
	}

	@Override
	public void subscribe(Subscriber<? super IN> subscriber) {
		long newId = uniqueId++;
		if ((options & SUBSCRIBE) == SUBSCRIBE) {
			if (log.isTraceEnabled()) {
				log.trace("subscribe: [" + newId + "] " + subscriber.getClass()
				                                                    .getSimpleName(), this);
			}
		}
		source.subscribe(new LoggerBarrier<>(this, newId, subscriber));
	}

	private final static class LoggerBarrier<IN> extends SubscriberBarrier<IN, IN> implements Named, Logging {

		private final int    options;
		private final Logger log;
		private final long   uniqueId;
		final private Level  level;

		private final FluxLog parent;

		public LoggerBarrier(FluxLog<IN> parent, long uniqueId, Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.parent = parent;
			this.level = parent.level;
			this.log = parent.log;
			this.options = parent.options;
			this.uniqueId = uniqueId;
		}

		private String concatId() {
			if (parent.uniqueId == 2L) {
				return "";
			}
			else {
				return "[" + uniqueId + "].";
			}
		}

		static private final String LOG_TEMPLATE = "{}({})";

		private void log(Object... args) {
			if (level == Level.FINEST) {
				log.trace(concatId() + " " + LOG_TEMPLATE, args);
			}
			else if (level == Level.FINE) {
				log.debug(concatId() + " " + LOG_TEMPLATE, args);
			}
			else if (level == Level.INFO) {
				log.info(concatId() + " " + LOG_TEMPLATE, args);
			}
			else if (level == Level.WARNING) {
				log.warn(concatId() + " " + LOG_TEMPLATE, args);
			}
			else if (level == Level.SEVERE) {
				log.error(concatId() + " " + LOG_TEMPLATE, args);
			}
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if ((options & ON_SUBSCRIBE) == ON_SUBSCRIBE && (level != Level.INFO || log.isInfoEnabled())) {
				log(SignalKind.onSubscribe, this.subscription, this);
			}
			subscriber.onSubscribe(this);
		}

		@Override
		protected void doNext(IN in) {
			if ((options & ON_NEXT) == ON_NEXT && (level != Level.INFO || log.isInfoEnabled())) {
				log(SignalKind.onNext, in, this);
			}
			subscriber.onNext(in);
		}

		@Override
		protected void doError(Throwable throwable) {
			if ((options & ON_ERROR) == ON_ERROR && log.isErrorEnabled()) {
				log.error(concatId() + " " + LOG_TEMPLATE, SignalKind.onError, throwable, this);
				log.error(concatId(), throwable);
			}
			subscriber.onError(throwable);
		}

		@Override
		protected void doOnSubscriberError(Throwable throwable) {
			doError(throwable);
		}

		@Override
		protected void doComplete() {
			if ((options & ON_COMPLETE) == ON_COMPLETE && (level != Level.INFO || log.isInfoEnabled())) {
				log(SignalKind.onComplete, "", this);
			}
			subscriber.onComplete();
		}

		@Override
		protected void doRequest(long n) {
			if ((options & REQUEST) == REQUEST && (level != Level.INFO || log.isInfoEnabled())) {
				log(SignalKind.request, Long.MAX_VALUE == n ? "unbounded" : n, this);
			}
			super.doRequest(n);
		}

		@Override
		protected void doCancel() {
			if ((options & CANCEL) == CANCEL && (level != Level.INFO || log.isInfoEnabled())) {
				log(SignalKind.cancel, "", this);
			}
			super.doCancel();
		}

		@Override
		public String getName() {
			return "/loggers/" + (log.getName()
			                         .equalsIgnoreCase(FluxLog.class.getName()) ? "default" :
					log.getName()) + "/" + uniqueId;
		}
	}

}
