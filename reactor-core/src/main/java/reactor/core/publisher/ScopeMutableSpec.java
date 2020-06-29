/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Mutable options for the {@link Flux#scope(Consumer, BiFunction)} operator, mainly around handling
 * {@link Context} and special hooks within the scope. Provided directly by the operator to a
 * {@link Consumer} for further user configuration. The result is then exposed in the operator as
 * a {@link ScopeContext} read-only object.
 */
public final class ScopeMutableSpec implements ScopeContext {

	private static Context TOTALLY_RESET(ContextView original, Context inner) {
		return Context.fromReadOnly(original);
	}

	/**
	 * Create a {@link ScopeMutableSpec} to be passed to the {@link Consumer} of {@link Flux#scope(Consumer, BiFunction) a scope}
	 * for further configuration.
	 *
	 * @return a new {@link ScopeMutableSpec} with no amends, total reset strategy, for the given original {@link Context}
	 */
	static final ScopeMutableSpec mutableSpec(ContextView original) {
		return new ScopeMutableSpec(Context.fromReadOnly(original), Context.empty(), ScopeMutableSpec::TOTALLY_RESET);
	}

	final Context                             original; //needs to be Context to allow putAll of amends
	Context                                   contextAmends;
	BiFunction<ContextView, Context, Context> contextRestoreStrategy; //original, inScope, restored

	private ScopeMutableSpec(final Context original, Context context, BiFunction<ContextView, Context, Context> contextRestoreStrategy) {
		this.original = original;
		this.contextAmends = context;
		this.contextRestoreStrategy = contextRestoreStrategy;
	}

	@Override
	public ContextView contextOutsideScope() {
		return this.original;
	}

	@Override
	public ContextView contextInScope() {
		return fullContextInScope();
	}

	Context fullContextInScope() {
		return this.original.putAll(this.contextAmends);
	}

	public ScopeContext readOnly() {
		return this;
	}

	/**
	 * Used for the simple strategy {@link #setRestoreStrategyToRemoveHooks()}.
	 */
	private Context removeHookKeys(ContextView original, Context inner) {
		return inner.delete(Hooks.KEY_ON_DISCARD);
	}

	/**
	 * Used for the intermediate strategy {@link #setRestoreStrategyToRemoveAllModifiedKeys()}.
	 */
	private Context removeAmendedKeys(ContextView original, Context inner) {
		final Context[] result = {inner};
		this.contextAmends.stream().forEach(e -> {
			result[0] = result[0].delete(e.getKey());
		});
		return result[0];
	}

	Context restore(Context contextInScope) {
		return this.contextRestoreStrategy.apply(original, contextInScope);
	}

	/**
	 * Outside (upstream) of the scope, the whole {@link Context} is reset to the value
	 * visible downstream of the scope. This includes discard hooks.
	 *
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec setRestoreStrategyToReset() {
		return setRestoreStrategyTo(ScopeMutableSpec::TOTALLY_RESET);
	}

	/**
	 * Outside (upstream) of the scope, the {@link Context} is rewritten to have the
	 * hooks set up in scope options removed. Free-form entries set via {@link #putInContext(Object, Object)}
	 * and {@link #putAllInContext(Context)} are preserved.
	 *
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec setRestoreStrategyToRemoveHooks() {
		return setRestoreStrategyTo(this::removeHookKeys);
	}

	/**
	 * Outside (upstream) of the scope, the whole {@link Context} is rewritten to <strong>remove</strong>
	 * all entries modified via the options. This includes discard hooks, but also free-form entries
	 * set via {@link #putInContext(Object, Object)} and {@link #putAllInContext(Context)}.
	 *
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec setRestoreStrategyToRemoveAllModifiedKeys() {
		return setRestoreStrategyTo(this::removeAmendedKeys);
	}

	/**
	 * Outside (upstream) of the scope, the whole {@link Context} is rewritten according to the provided
	 * {@link BiFunction}, which receives the original {@link ContextView} as visible downstream of the scope and the
	 * modified {@link Context} as visible from within the scope.
	 *
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec setRestoreStrategyTo(BiFunction<ContextView, Context, Context> strategyFromOriginalAndInner) {
		this.contextRestoreStrategy = strategyFromOriginalAndInner;
		return this;
	}

	/**
	 * Augment the {@link Context} in this scope with an additional discard hook.
	 * //TODO give more details on discard hooks and how they combine
	 *
	 * @param discardHook
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec addDiscardHandler(Consumer<?> discardHook) {
		this.contextAmends = Operators.addOnDiscard(contextAmends, discardHook);
		return this;
	}

	/**
	 * Augment the {@link Context} in this scope with an additional type-safe discard hook.
	 * //TODO give more details on discard hooks and how they combine
	 *
	 * @param clazz
	 * @param discardHook
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public <T> ScopeMutableSpec addDiscardHandler(Class<T> clazz, Consumer<? super T> discardHook) {
		this.contextAmends = Operators.discardLocalAdapter(clazz, discardHook).apply(contextAmends);
		return this;
	}

	/**
	 * Augment the {@link Context} in this scope with a custom entry, replacing any value that would
	 * previously be set for the given key.
	 *
	 * @param key the key to add/replace
	 * @param value the value for the key
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec putInContext(Object key, Object value) {
		this.contextAmends = contextAmends.put(key, value);
		return this;
	}

	/**
	 * Augment the {@link Context} in this scope with custom entries, replacing any value that would
	 * previously be set for the given keys.
	 *
	 * @param entries the {@link Context} from which to {@link Context#putAll(Context) add/replace all} entries
	 * @return this {@link ScopeMutableSpec} for further setup
	 */
	public ScopeMutableSpec putAllInContext(Context entries) {
		this.contextAmends = this.contextAmends.putAll(entries);
		return this;
	}
}
