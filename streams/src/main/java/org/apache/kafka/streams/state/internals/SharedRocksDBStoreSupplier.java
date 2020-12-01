package org.apache.kafka.streams.state.internals;

import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class SharedRocksDBStoreSupplier implements KeyValueBytesStoreSupplier {
  private final String name;

  public SharedRocksDBStoreSupplier(final String name) {
    this.name = Objects.requireNonNull(name, "name");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public KeyValueStore<Bytes, byte[]> get() {
    return new SharedRocksDBStore(name);
  }

  @Override
  public String metricsScope() {
    return "rocksdb";
  }
}

