/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdks.state.hazelcast;

import java.io.Serializable;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.sdk.coders.Coder;

import com.hazelcast.core.HazelcastInstance;

/**
 * The state factory backed by hazelcast.
 * Important: a runner must support to close the state factory to be able to use this impl.
 * @param <K> the key type.
 */
public class HazelcastStateFactory<K> implements StateInternalsFactory<K>, AutoCloseable, Serializable {
    private Coder<K> coder;
    private HazelcastStateOptions options;
    private transient volatile VolatileState state;

    public HazelcastStateFactory(final HazelcastStateOptions options, final Coder<K> coder) {
        this.options = options;
        this.coder = coder;
    }

    @Override
    public StateInternals stateInternalsForKey(final K key) {
        enforceInstance();
        return new HazelcastStateImpl<>(this, coder, Coders.encode(coder, key));
    }

    @Override
    public synchronized void close() {
        if (state == null || state.closed) {
            return;
        }

        state.closed = true;
        if (state.instance == null) {
            state.instance.getLifecycleService().shutdown();
            state.instance = null;
        }
    }

    String prefix() {
        return options.getHazelcastStateStorePrefix();
    }

    HazelcastInstance instance() {
        enforceInstance();
        return state.instance;
    }

    private void enforceInstance() {
        if (state == null) {
            synchronized (this) {
                if (state == null) {
                    state = new VolatileState();
                    state.instance = HazelcastInstanceFactory.create(options);
                }
            }
        } else if (state.closed) {
            throw new IllegalStateException("Instance already closed");
        }
    }

    private static class VolatileState {
        private transient volatile HazelcastInstance instance;
        private transient volatile boolean closed;
    }
}
