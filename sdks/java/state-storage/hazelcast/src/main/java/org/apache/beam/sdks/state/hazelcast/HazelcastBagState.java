package org.apache.beam.sdks.state.hazelcast;

import java.util.Collection;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;

class HazelcastBagState<T> extends BaseHazelcastCollectionState<T> implements BagState<T> {
  HazelcastBagState(final String id, final byte[] key, final Coder<T> coder, final HazelcastStateImpl<?> state) {
    super(id, key, coder, state);
  }

  @Override
  public BagState<T> readLater() {
    return this;
  }

  protected HazelcastWrapper.SimplePayload toValue(T t) {
    final HazelcastWrapper.SimplePayload value = new HazelcastWrapper.SimplePayload();
    value.setPayload(Coders.encode(coder, t));
    return value;
  }

  protected Collection<HazelcastWrapper> bag() {
    return state.getFactory().instance().getList(state.getFactory().prefix() + "@list@" + id);
  }
}
