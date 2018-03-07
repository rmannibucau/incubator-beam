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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.beam.sdk.coders.Coder;

/**
 * Just a small helper to simplify the encoding/decoding in the module.
 */
public final class Coders {
    private Coders() {
        // no-op
    }

    public static <K> K decode(final Coder<K> coder, final byte[] key) {
        try {
            return coder.decode(new ByteArrayInputStream(key));
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> byte[] encode(final Coder<T> coder, final T value) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            coder.encode(value, byteArrayOutputStream);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }
}
