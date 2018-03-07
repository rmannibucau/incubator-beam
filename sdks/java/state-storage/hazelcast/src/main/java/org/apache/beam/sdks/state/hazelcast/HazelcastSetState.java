package org.apache.beam.sdks.state.hazelcast;

import java.util.Collection;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;

class HazelcastSetState<T> extends BaseHazelcastCollectionState<T> implements SetState<T> {
  HazelcastSetState(final String id, final byte[] key, final Coder<T> coder, final HazelcastStateImpl<?> state) {
    super(id, key, coder, state);
  }

  @Override
  protected Collection<HazelcastWrapper> bag() {
    return state.getFactory().instance().getSet(state.getFactory().prefix() + "@set@" + id);
  }

  @Override
  public SetState<T> readLater() {
    return this;
  }

  @Override
  public ReadableState<Boolean> contains(final T t) {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return bag().contains(toValue(t));
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<Boolean> addIfAbsent(final T t) {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return bag().add(toValue(t));
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }

  @Override
  public void remove(final T t) {
    bag().remove(toValue(t));
  }
}
