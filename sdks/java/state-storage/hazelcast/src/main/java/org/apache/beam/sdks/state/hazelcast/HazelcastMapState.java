package org.apache.beam.sdks.state.hazelcast;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.Base64;
import java.util.Map;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;

import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

class HazelcastMapState<K, V> extends BaseHazelcastState<K> implements MapState<K, V> {
  private final Coder<V> valueCoder;

  HazelcastMapState(final String id, final byte[] key, final Coder<K> keyCoder, final Coder<V> valueCoder, final HazelcastStateImpl<?> state) {
    super(id, key, keyCoder, state);
    this.valueCoder = valueCoder;
  }

  @Override
  public void put(final K key, final V value) {
    map().put(toKey(key), toValue(value));
  }

  @Override
  public ReadableState<V> putIfAbsent(final K key, final V value) {
    return new ReadableState<V>() {
      @Override
      public V read() {
        final HazelcastWrapper.SimplePayload simplePayload = map().putIfAbsent(toKey(key), toValue(value));
        return simplePayload == null ? null : Coders.decode(valueCoder, simplePayload.getPayload());
      }

      @Override
      public ReadableState<V> readLater() {
        return this;
      }
    };
  }

  @Override
  public void remove(final K key) {
    map().remove(toKey(key));
  }

  @Override
  public ReadableState<V> get(final K key) {
    return new ReadableState<V>() {
      @Override
      public V read() {
        final HazelcastWrapper.SimplePayload simplePayload = map().get(toKey(key));
        return simplePayload == null ? null : Coders.decode(valueCoder, simplePayload.getPayload());
      }

      @Override
      public ReadableState<V> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<Iterable<K>> keys() {
    return new ReadableState<Iterable<K>>() {
      @Override
      public Iterable<K> read() {
        return map().keySet(filter()).stream()
                .map(k -> Coders.decode(coder, k.getMapKey()))
                .collect(toSet());
      }

      @Override
      public ReadableState<Iterable<K>> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<Iterable<V>> values() {
    return new ReadableState<Iterable<V>>() {
      @Override
      public Iterable<V> read() {
        return map().values(filter()).stream()
                    .map(k -> Coders.decode(valueCoder, k.getPayload()))
                    .collect(toList());
      }

      @Override
      public ReadableState<Iterable<V>> readLater() {
        return this;
      }
    };
  }

  @Override
  public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
    return new ReadableState<Iterable<Map.Entry<K, V>>>() {
      @Override
      public Iterable<Map.Entry<K, V>> read() {
        return map().entrySet(filter())
                    .stream()
                    .collect(toMap(e -> Coders.decode(coder, e.getKey().getMapKey()),
                                 e -> Coders.decode(valueCoder, e.getValue().getPayload())))
                    .entrySet();
      }

      @Override
      public ReadableState<Iterable<Map.Entry<K, V>>> readLater() {
        return this;
      }
    };
  }

  @Override
  public void clear() {
    map().removeAll(filter());
  }

  private Predicate<HazelcastWrapper.MapKey, HazelcastWrapper.SimplePayload> filter() {
    return Predicates.equal("key", keyAsString());
  }

  private HazelcastWrapper.MapKey toKey(final K value) {
    final HazelcastWrapper.MapKey wrapper = new HazelcastWrapper.MapKey();
    wrapper.setKey(keyAsString());
    wrapper.setMapKey(Coders.encode(coder, value));
    return wrapper;
  }

  private String keyAsString() {
    return Base64.getEncoder().encodeToString(key);
  }

  private HazelcastWrapper.SimplePayload toValue(final V value) {
    final HazelcastWrapper.SimplePayload wrapper = new HazelcastWrapper.SimplePayload();
    wrapper.setPayload(Coders.encode(valueCoder, value));
    return wrapper;
  }

  private IMap<HazelcastWrapper.MapKey, HazelcastWrapper.SimplePayload> map() {
    return state.getFactory().instance().getMap(state.getFactory().prefix() + "@map@" + id);
  }
}
