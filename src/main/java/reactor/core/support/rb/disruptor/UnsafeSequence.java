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

import reactor.core.support.ReactiveState;
import reactor.core.support.internal.PlatformDependent0;
import reactor.fn.LongSupplier;
import sun.misc.Unsafe;


class LhsPadding
{
    protected long p1, p2, p3, p4, p5, p6, p7;
}

class Value extends LhsPadding
{
    protected volatile long value;
}

class RhsPadding extends Value
{
    protected long p9, p10, p11, p12, p13, p14, p15;
}

/**
 * <p>Concurrent sequence class used for tracking the progress of
 * the ring buffer and event processors.  Support a number
 * of concurrent operations including CAS and order writes.
 *
 * <p>Also attempts to be more efficient with regards to false
 * sharing by adding padding around the volatile field.
 */
public class UnsafeSequence extends RhsPadding implements Sequence, LongSupplier, ReactiveState.Trace
{
    private static final Unsafe UNSAFE;
    private static final long VALUE_OFFSET;

    static
    {
        UNSAFE = PlatformDependent0.getUnsafe();
        try
        {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Value.class.getDeclaredField("value"));
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a sequence with a specified initial value.
     *
     * @param initialValue The initial value for this sequence.
     */
    public UnsafeSequence(final long initialValue)
    {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, initialValue);
    }

    @Override
    public long get()
    {
        return value;
    }

    @Override
    public void set(final long value)
    {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    @Override
    public void setVolatile(final long value)
    {
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }

    @Override
    public boolean compareAndSet(final long expectedValue, final long newValue)
    {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedValue, newValue);
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
