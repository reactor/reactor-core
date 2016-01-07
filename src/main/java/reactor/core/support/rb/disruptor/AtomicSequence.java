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
package reactor.core.support.rb.disruptor;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import reactor.core.support.ReactiveState;
import reactor.fn.LongSupplier;

/**
 * <p>Concurrent sequence class used for tracking the progress of
 * the ring buffer and event processors.  Support a number
 * of concurrent operations including CAS and order writes.
 *
 * <p>Also attempts to be more efficient with regards to false
 * sharing by adding padding around the volatile field.
 */
public class AtomicSequence extends RhsPadding implements LongSupplier, Sequence, ReactiveState.Trace
{

    private static final AtomicLongFieldUpdater<Value> UPDATER =
      AtomicLongFieldUpdater.newUpdater(Value.class, "value");

    /**
     * Create a sequence with a specified initial value.
     *
     * @param initialValue The initial value for this sequence.
     */
    AtomicSequence(final long initialValue)
    {
        UPDATER.lazySet(this, initialValue);
    }

    @Override
    public long get()
    {
        return value;
    }

    @Override
    public void set(final long value)
    {
        UPDATER.set(this, value);
    }

    @Override
    public void setVolatile(final long value)
    {
        UPDATER.lazySet(this, value);
    }

    @Override
    public boolean compareAndSet(final long expectedValue, final long newValue)
    {
        return UPDATER.compareAndSet(this, expectedValue, newValue);
    }

    @Override
    public long incrementAndGet()
    {
        return addAndGet(1L);
    }

    @Override
    public long addAndGet(final long increment)
    {
        long currentValue;
        long newValue;

        do
        {
            currentValue = get();
            newValue = currentValue + increment;
        }
        while (!compareAndSet(currentValue, newValue));

        return newValue;
    }

    @Override
    public String toString()
    {
        return Long.toString(get());
    }
}
