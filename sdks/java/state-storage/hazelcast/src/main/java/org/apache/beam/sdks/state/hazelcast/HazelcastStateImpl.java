/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;

@Experimental(Experimental.Kind.STATE)
public class HazelcastStateImpl<K> implements StateInternals, Serializable {

    private HazelcastStateFactory<K> factory;
    private Coder<K> coder;
    private byte[] key;

    HazelcastStateImpl(final HazelcastStateFactory<K> factory,
                       final Coder<K> coder,
                       final byte[] key) {
        this.factory = factory;
        this.coder = coder;
        this.key = key;
    }

    @Override
    public K getKey() {
        return Coders.decode(coder, key);
    }

    @Override
    public <T extends State> T state(final StateNamespace namespace, final StateTag<T> address) {
        return state(namespace, address, StateContexts.nullContext());
    }

    @Override
    public <T extends State> T state(final StateNamespace namespace, final StateTag<T> address, final StateContext<?> c) {
        return address.getSpec().bind(address.getId(), new HazelastStateBinder());
    }

    HazelcastStateFactory<K> getFactory() {
        return factory;
    }

    private class HazelastStateBinder implements StateBinder {
        @Override
        public <T> ValueState<T> bindValue(final String id, final StateSpec<ValueState<T>> spec, final Coder<T> coder) {
            return new HazelcastValueState<>(id, key, coder, HazelcastStateImpl.this);
        }

        @Override
        public <T> BagState<T> bindBag(final String id, final StateSpec<BagState<T>> spec, final Coder<T> elemCoder) {
            return new HazelcastBagState<>(id, key, elemCoder, HazelcastStateImpl.this);
        }

        @Override
        public <T> SetState<T> bindSet(final String id, final StateSpec<SetState<T>> spec, final Coder<T> elemCoder) {
            return new HazelcastSetState<>(id, key, elemCoder, HazelcastStateImpl.this);
        }

        @Override
        public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(final String id, final StateSpec<MapState<KeyT, ValueT>> spec,
                final Coder<KeyT> mapKeyCoder, final Coder<ValueT> mapValueCoder) {
            return new HazelcastMapState<>(id, key, mapKeyCoder, mapValueCoder, HazelcastStateImpl.this);
        }

        @Override
        public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(final String id,
                final StateSpec<CombiningState<InputT, AccumT, OutputT>> spec, final Coder<AccumT> accumCoder,
                final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
            return null; // TODO
        }

        @Override
        public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningWithContext(final String id,
                final StateSpec<CombiningState<InputT, AccumT, OutputT>> spec, final Coder<AccumT> accumCoder,
                final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            return null; // TODO
        }

        @Override
        public WatermarkHoldState bindWatermark(final String id, final StateSpec<WatermarkHoldState> spec,
                final TimestampCombiner timestampCombiner) {
            return null; // TODO
        }
    }
}
