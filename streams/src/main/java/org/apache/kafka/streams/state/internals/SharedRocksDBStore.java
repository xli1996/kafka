package org.apache.kafka.streams.state.internals;

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class SharedRocksDBStore implements KeyValueStore<Bytes, byte[]> {
  private static final Object lk = new Object();
  private static final ThreadLocal<KeyValueStore<Bytes, byte[]>> wrappedStore = new ThreadLocal<>();
  private static final AtomicInteger prefixGen = new AtomicInteger(0);
  private static final ThreadLocal<BatchingStateRestoreCallback> restoreCallback
      = new ThreadLocal<>();

  private final String name;
  private final byte[] keyPrefix;

  private enum StorePath {
    STATE_STORE_PATH("/mnt/data/data/state-store"),
    TMP_PATH("/tmp/state-store");

    final String path;

    StorePath(final String path) {
      this.path = path;
    }
  }

  SharedRocksDBStore(final String name) {
    this.name = Objects.requireNonNull(name, "name");
    // just pick a random UUID for now
    final int prefix = prefixGen.incrementAndGet();
    final ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(prefix);
    this.keyPrefix = bb.array();
  }

  private static void initStore(final ProcessorContext processorContext) {
    synchronized (lk) {
      if (wrappedStore.get() == null) {
        wrappedStore.set(newStore(processorContext));
      }
    }
  }

  @Override
  public void init(final ProcessorContext processorContext, final StateStore stateStore) {
    initStore(processorContext);
    processorContext.register(stateStore, new RestoreCallback(restoreCallback.get()));
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void flush() {
    getWrappedStore().flush();
  }

  @Override
  public void close() {
    // no-op for now
  }

  @Override
  public boolean persistent() {
    return true;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
    return getWrappedStore().reverseRange(wrappedKey(from), wrappedKey(to));
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> reverseAll() {
    return getWrappedStore().reverseRange(
        wrappedKey(Bytes.wrap(new byte[]{0})), wrappedKey(Bytes.wrap(new byte[]{Byte.MAX_VALUE}))
    );
  }

  @Override
  public byte[] get(final Bytes key) {
    return getWrappedStore().get(wrappedKey(key));
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
    return getWrappedStore().range(wrappedKey(from), wrappedKey(to));
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return getWrappedStore().range(
        wrappedKey(Bytes.wrap(new byte[]{0})), wrappedKey(Bytes.wrap(new byte[]{Byte.MAX_VALUE}))
    );
  }

  @Override
  public long approximateNumEntries() {
    return 0;
  }

  @Override
  public void put(final Bytes key, final byte[] value) {
    getWrappedStore().put(wrappedKey(key), value);
  }

  @Override
  public byte[] putIfAbsent(final Bytes key, final byte[] value) {
    return getWrappedStore().putIfAbsent(wrappedKey(key), value);
  }

  @Override
  public void putAll(final List<KeyValue<Bytes, byte[]>> list) {
    final List<KeyValue<Bytes, byte[]>> withWrappedKey = list.stream()
        .map(kv -> KeyValue.pair(wrappedKey(kv.key), kv.value))
        .collect(Collectors.toList());
    getWrappedStore().putAll(withWrappedKey);
  }

  @Override
  public byte[] delete(final Bytes key) {
    return getWrappedStore().delete(wrappedKey(key));
  }

  private Bytes wrappedKey(final Bytes key) {
    final byte[] wrapped = new byte[key.get().length + keyPrefix.length];
    System.arraycopy(keyPrefix, 0, wrapped, 0, keyPrefix.length);
    System.arraycopy(key.get(), 0, wrapped, keyPrefix.length, key.get().length);
    return Bytes.wrap(wrapped);
  }

  private KeyValueStore<Bytes, byte[]> getWrappedStore() {
    return wrappedStore.get();
  }

  private static KeyValueStore<Bytes, byte[]> newStore(final ProcessorContext processorContext) {
    final KeyValueStore<Bytes, byte[]> store = new RocksDbKeyValueBytesStoreSupplier(
        "RocksDB-" + Thread.currentThread().getName(),
        false
    ).get();
    final WrappedContext context = new WrappedContext(
        processorContext.metrics(), processorContext.appConfigs());
    store.init(context, store);
    return store;
  }

  private static class WrappedContext implements ProcessorContext {
    private final StreamsMetrics streamsMetrics;
    private final Map<String, Object> appConfig;

    private WrappedContext(
        final StreamsMetrics streamsMetrics,
        final Map<String, Object> appConfig
    ) {
      this.streamsMetrics = Objects.requireNonNull(streamsMetrics, "streamsMetrics");
      this.appConfig = Objects.requireNonNull(appConfig, "appConfig");
    }

    public String applicationId() {
      throw new UnsupportedOperationException();
    }

    public TaskId taskId() {
      return new TaskId(0, 0);
    }

    public Serde<?> keySerde() {
      throw new UnsupportedOperationException();
    }

    public Serde<?> valueSerde() {
      throw new UnsupportedOperationException();
    }

    public File stateDir() {
      String path = System.getenv().get("STORE_PATH");
      if (path == null) {
        path = StorePath.STATE_STORE_PATH.path;
      }
      return new File(path);
    }

    public StreamsMetrics metrics() {
      return streamsMetrics;
    }

    public void register(StateStore root, StateRestoreCallback callback) {
      restoreCallback.set((BatchingStateRestoreCallback) callback);
    }

    @Override
    public StateStore getStateStore(String s) {
      throw new UnsupportedOperationException();
    }

    /** @deprecated */
    @Deprecated
    public Cancellable schedule(long var1, PunctuationType var3, Punctuator var4) {
      throw new UnsupportedOperationException();
    }

    public Cancellable schedule(Duration var1, PunctuationType var2, Punctuator var3) {
      throw new UnsupportedOperationException();
    }

    public <K, V> void forward(K var1, V var2) {
      throw new UnsupportedOperationException();
    }

    public <K, V> void forward(K var1, V var2, To var3) {
      throw new UnsupportedOperationException();
    }

    /** @deprecated */
    @Deprecated
    public <K, V> void forward(K var1, V var2, int var3) {
      throw new UnsupportedOperationException();
    }

    /** @deprecated */
    @Deprecated
    public <K, V> void forward(K var1, V var2, String var3) {
      throw new UnsupportedOperationException();
    }

    public void commit() {
      throw new UnsupportedOperationException();
    }

    public String topic() {
      throw new UnsupportedOperationException();
    }

    public int partition() {
      throw new UnsupportedOperationException();
    }

    public long offset() {
      throw new UnsupportedOperationException();
    }

    public Headers headers() {
      throw new UnsupportedOperationException();
    }

    public long timestamp() {
      throw new UnsupportedOperationException();
    }

    public Map<String, Object> appConfigs() {
      return appConfig;
    }

    public Map<String, Object> appConfigsWithPrefix(String var1) {
      throw new UnsupportedOperationException();
    }
  }

  private class RestoreCallback implements BatchingStateRestoreCallback {
    private final BatchingStateRestoreCallback inner;

    private RestoreCallback(final BatchingStateRestoreCallback inner) {
      this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
      final List<KeyValue<byte[], byte[]>> wrapped = records.stream()
          .map(r -> KeyValue.pair(wrappedKey(Bytes.wrap(r.key)).get(), r.value))
          .collect(Collectors.toList());
      inner.restoreAll(wrapped);
    }
  }
}