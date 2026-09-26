/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.scheduler;

/**
 * A marker interface for {@link Runnable} tasks that must be submitted to a
 * {@link Scheduler} without the decoration registered via
 * {@link Schedulers#onScheduleHook(String, java.util.function.Function)}:
 * {@link Schedulers#onSchedule(Runnable)} returns such tasks as-is.
 *
 * <p>Intended for library-internal, self-rescheduling maintenance tasks (for example
 * background resource eviction) that never execute user code. Decorating such a task
 * with a hook that captures caller state (such as the context-capture hook installed by
 * {@link reactor.core.publisher.Hooks#enableAutomaticContextPropagation()}) stores the
 * scheduling caller's {@code ThreadLocal} state (typically request-scoped, because such
 * resources are usually created lazily on request processing threads) in the pending
 * task. A task that re-schedules itself from within its own execution then re-captures
 * that state on every reschedule, retaining the caller's object graph for the lifetime
 * of the resource instead of the lifetime of the request.
 *
 * <p>User-facing tasks should not implement this interface: skipping decoration also
 * skips user-registered hooks such as MDC propagation.
 *
 * @since 3.8.7
 */
public interface UndecoratedRunnable extends Runnable {
}
