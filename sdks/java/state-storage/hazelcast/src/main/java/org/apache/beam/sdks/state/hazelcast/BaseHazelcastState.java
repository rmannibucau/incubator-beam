package org.apache.beam.sdks.state.hazelcast;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;

abstract class BaseHazelcastState<T> {
  protected final String id;
  protected final byte[] key;
  protected final Coder<T> coder;
  protected final HazelcastStateImpl<?> state;
  protected transient HazelcastKey cachedkey;

  BaseHazelcastState(final String id, final byte[] key, final Coder<T> coder, final HazelcastStateImpl<?> state) {
    this.id = id;
    this.key = key;
    this.coder = coder;
    this.state = state;
  }

  protected synchronized HazelcastKey key() {
    return cachedkey == null ? cachedkey = new HazelcastKey(id, key) : cachedkey;
  }
}
