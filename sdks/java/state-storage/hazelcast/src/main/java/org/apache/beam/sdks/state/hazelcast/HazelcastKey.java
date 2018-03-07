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

public class HazelcastKey implements Serializable {
    private String id;
    private byte[] key;

    private transient volatile Integer hash;

    public HazelcastKey(final String id, final byte[] key) {
        this.id = id;
        this.key = key;

        hashCode(); // force init
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HazelcastKey that = HazelcastKey.class.cast(o);
        return Objects.equals(id, that.id) && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        if (hash == null) {
            synchronized (this) {
                if (hash == null) {
                    hash = 31 * Objects.hash(id) + Arrays.hashCode(key);
                }
            }
        }
        return hash;
    }
}
