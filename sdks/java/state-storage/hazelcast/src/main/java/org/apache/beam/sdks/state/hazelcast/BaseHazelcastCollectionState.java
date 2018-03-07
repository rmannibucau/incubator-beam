package org.apache.beam.sdks.state.hazelcast;

import static java.util.stream.Collectors.toList;

import java.util.Collection;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ReadableState;

abstract class BaseHazelcastCollectionState<T> extends BaseHazelcastState<T> {
  BaseHazelcastCollectionState(final String id, final byte[] key, final Coder<T> coder, final HazelcastStateImpl<?> state) {
    super(id, key, coder, state);
  }

  public Iterable<T> read() {
    return bag().stream()
                 .map(v -> Coders.decode(coder, HazelcastWrapper.SimplePayload.class.cast(v).getPayload()))
                 .collect(toList());
  }

  public void add(final T value) {
    bag().add(toValue(value));
  }

  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return bag().isEmpty();
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  public void clear() {
    bag().clear();
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
