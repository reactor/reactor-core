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
package reactor.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.publisher.Signal;
import reactor.test.ValueFormatters.Extractor;
import reactor.test.ValueFormatters.ToStringConverter;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Options for a {@link StepVerifier}, including the initial request amount,
 * {@link VirtualTimeScheduler} supplier and toggles for some checks.
 *
 * @author Simon Basle
 */
public class StepVerifierOptions {

	@Nullable
	private String scenarioName = null;

	private boolean checkUnderRequesting = true;
	private long initialRequest = Long.MAX_VALUE;
	private Supplier<? extends VirtualTimeScheduler> vtsLookup = null;
	private Context initialContext;

	@Nullable
	private ToStringConverter objectFormatter = null;

	final Map<Class<?>, Extractor<?>> extractorMap = new LinkedHashMap<>();

	/**
	 * Create a new default set of options for a {@link StepVerifier} that can be tuned
	 * using the various available non-getter methods (which can be chained).
	 */
	public static StepVerifierOptions create() {
		return new StepVerifierOptions();
	}

	private StepVerifierOptions() { } //disable constructor

	/**
	 * Activate or deactivate the {@link StepVerifier} check of request amount
	 * being too low. Defauts to true.
	 *
	 * @param enabled true if the check should be enabled.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions checkUnderRequesting(boolean enabled) {
		this.checkUnderRequesting = enabled;
		return this;
	}

	/**
	 * @return true if the {@link StepVerifier} receiving these options should activate
	 * the check of request amount being too low.
	 */
	public boolean isCheckUnderRequesting() {
		return this.checkUnderRequesting;
	}

	/**
	 * Set the amount the {@link StepVerifier} should request initially. Defaults to
	 * unbounded request ({@code Long.MAX_VALUE}).
	 *
	 * @param initialRequest the initial request amount.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions initialRequest(long initialRequest) {
		this.initialRequest = initialRequest;
		return this;
	}

	/**
	 * @return the initial request amount to be made by the {@link StepVerifier}
	 * receiving these options.
	 */
	public long getInitialRequest() {
		return this.initialRequest;
	}

	/**
	 * Set up a custom value formatter to be used in error messages when presenting
	 * expected and actual values. This is intended for classes that have obscure {@link #toString()}
	 * implementation that cannot be overridden.
	 * <p>
	 * This is a {@link Function} capable of formatting an arbitrary {@link Object} to
	 * {@link String}, with the intention of detecting elements from the sequence under
	 * test and applying customized {@link String} conversion to them (and simply calling
	 * {@link #toString()} on other objects).
	 * <p>
	 * See {@link ValueFormatters} for factories of such functions.
	 *
	 * @param valueFormatter the custom value to {@link String} formatter, or null to deactivate
	 * custom formatting
	 * @return this instance, to continue setting the options
	 */
	public StepVerifierOptions valueFormatter(
			@Nullable ToStringConverter valueFormatter) {
		this.objectFormatter = valueFormatter;
		return this;
	}

	/**
	 * Get the custom object formatter to use when producing messages. The formatter
	 * should be able to work with any {@link Object}, usually filtering types matching
	 * the content of the sequence under test, and applying a simple {@link String} conversion
	 * on other objects.
	 *
	 * @return the custom value formatter, or null if no specific formatting has been defined.
	 */
	@Nullable
	public ToStringConverter getValueFormatter() {
		return this.objectFormatter;
	}

	/**
	 * Add an {@link Extractor}, replacing any existing {@link Extractor} that targets the
	 * same {@link Class} (as in {@link Extractor#getTargetClass()}).
	 * <p>
	 * Note that by default, default extractors for {@link ValueFormatters#signalExtractor() Signal},
	 * {@link ValueFormatters#iterableExtractor() Iterable} and
	 * {@link ValueFormatters#arrayExtractor(Class) Object[]} are in place.
	 *
	 *
	 * @param extractor the extractor to add / set
	 * @param <T> the type of container considered by this extractor
	 * @return this instance, to continue setting the options
	 */
	public <T> StepVerifierOptions extractor(Extractor<T> extractor) {
		extractorMap.put(extractor.getTargetClass(), extractor);
		return this;
	}

	/**
	 * Get the list of value extractors added to the options, including default ones at
	 * the end (unless they've been individually replaced).
	 * <p>
	 * The {@link Collection} is a copy, and mutating the collection doesn't mutate the
	 * configured extractors in this {@link StepVerifierOptions}.
	 *
	 * @return the collection of value {@link Extractor}
	 */
	public Collection<Extractor<?>> getExtractors() {
		ArrayList<Extractor<?>> copy = new ArrayList<>(extractorMap.size() + 3);

		copy.addAll(extractorMap.values());
		if (!extractorMap.containsKey(Signal.class)) copy.add(ValueFormatters.signalExtractor());
		if (!extractorMap.containsKey(Iterable.class)) copy.add(ValueFormatters.iterableExtractor());
		if (!extractorMap.containsKey(Object[].class)) copy.add(ValueFormatters.arrayExtractor(Object[].class));
		return copy;
	}

	/**
	 * Set a supplier for a {@link VirtualTimeScheduler}, which is mandatory for a
	 * {@link StepVerifier} to work with virtual time. Defaults to null.
	 *
	 * @param vtsLookup the supplier of {@link VirtualTimeScheduler} to use.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions virtualTimeSchedulerSupplier(Supplier<? extends VirtualTimeScheduler> vtsLookup) {
		this.vtsLookup = vtsLookup;
		return this;
	}

	/**
	 * @return the supplier of {@link VirtualTimeScheduler} to be used by the
	 * {@link StepVerifier} receiving these options.
	 *
	 */
	@Nullable
	public Supplier<? extends VirtualTimeScheduler> getVirtualTimeSchedulerSupplier() {
		return vtsLookup;
	}

	/**
	 * Set an initial {@link Context} to be propagated by the {@link StepVerifier} when it
	 * subscribes to the sequence under test.
	 *
	 * @param context the {@link Context} to propagate.
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions withInitialContext(Context context) {
		this.initialContext = context;
		return this;
	}

	/**
	 * @return the {@link Context} to be propagated initially by the {@link StepVerifier}.
	 */
	@Nullable
	public Context getInitialContext() {
		return this.initialContext;
	}

	/**
	 * Give a name to the whole scenario tested by the configured {@link StepVerifier}. That
	 * name would be mentioned in exceptions and assertion errors raised by the StepVerifier,
	 * allowing to better distinguish error sources in unit tests where multiple StepVerifier
	 * are used.
	 *
	 * @param scenarioName the name of the scenario, null to deactivate
	 * @return this instance, to continue setting the options.
	 */
	public StepVerifierOptions scenarioName(@Nullable String scenarioName) {
		this.scenarioName = scenarioName;
		return this;
	}

	/**
	 * @return the name given to the configured {@link StepVerifier}, or null if none.
	 */
	@Nullable
	public String getScenarioName() {
		return this.scenarioName;
	}
}
