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
import java.util.Arrays;
import java.util.Objects;

public abstract class HazelcastWrapper implements Serializable {
    public static class SimplePayload extends HazelcastWrapper {

        private byte[] payload;

        public byte[] getPayload() {
            return payload;
        }

        public void setPayload(final byte[] payload) {
            this.payload = payload;
        }

        @Override
        public boolean equals(final Object o) {
            return this == o || o != null && getClass() == o.getClass() && Arrays.equals(payload,
                    SimplePayload.class.cast(o).payload);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(payload);
        }
    }

    public static class MapKey extends HazelcastWrapper {

        private String key;
        private byte[] mapKey;

        public String getKey() {
            return key;
        }

        public void setKey(final String key) {
            this.key = key;
        }

        public byte[] getMapKey() {
            return mapKey;
        }

        public void setMapKey(final byte[] mapKey) {
            this.mapKey = mapKey;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final MapKey key = MapKey.class.cast(o);
            return Objects.equals(this.key, key.key) && Arrays.equals(mapKey, key.mapKey);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hashCode(key) + Arrays.hashCode(mapKey);
        }
    }
}
