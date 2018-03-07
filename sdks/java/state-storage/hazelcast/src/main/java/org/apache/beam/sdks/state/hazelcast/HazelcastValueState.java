package org.apache.beam.sdks.state.hazelcast;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ValueState;

import com.hazelcast.core.IMap;

class HazelcastValueState<T> extends BaseHazelcastState<T> implements ValueState<T> {
  HazelcastValueState(final String id, final byte[] key, final Coder<T> coder, final HazelcastStateImpl<?> state) {
    super(id, key, coder, state);
  }

  @Override
  public void write(final T input) {
    final HazelcastWrapper.SimplePayload value = new HazelcastWrapper.SimplePayload();
    value.setPayload(Coders.encode(coder, input));
    map().put(key(), value);
  }

  @Override
  public T read() {
    final HazelcastWrapper.SimplePayload value = HazelcastWrapper.SimplePayload.class.cast(map().get(key()));
    return Coders.decode(coder, value.getPayload());
  }

  @Override
  public void clear() {
    map().remove(key());
  }

  @Override
  public ValueState<T> readLater() {
    return this;
  }

  private IMap<HazelcastKey, HazelcastWrapper.SimplePayload> map() {
    return state.getFactory().instance().getMap(state.getFactory().prefix() + "@value@" + id);
  }
}
