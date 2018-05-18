/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.test;

import java.util.function.Supplier;

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

	private boolean checkUnderRequesting = true;
	private long initialRequest = Long.MAX_VALUE;
	private Supplier<? extends VirtualTimeScheduler> vtsLookup = null;
	private Context initialContext;

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
}
