package dido.table.internal;

import dido.data.*;
import dido.data.partial.PartialData;
import dido.data.util.FieldValuesIn;
import dido.flow.*;
import dido.table.ConcurrentTable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ConcurrentTableBasic<K extends Comparable<K>>
        implements ConcurrentTable<K>, KeyPublisher<K>, KeyedDataConsumer<K> {

    private final DataTableBasic<K> delegate;

    private final DidoTransform copy;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    protected ConcurrentTableBasic(DataTableBasic<K> delegate,
                                   DidoTransform copy) {
        this.delegate = delegate;
        this.copy = copy;
    }

    public static <K extends Comparable<K>> ConcurrentTableBasic<K>
    create(DataSchema schema,
           DataFactoryProvider dataFactoryProvider) {

        DataTableBasic<K> tableBasic = DataTableBasic.forSchema(schema);

        DataFactory dataFactory = dataFactoryProvider.factoryFor(schema);

        FromValues fromValues = FieldValuesIn.withDataFactory(dataFactory);

        DidoTransform copy = fromValues.toCopyFunction(tableBasic.getSchema());

        return new ConcurrentTableBasic<>(tableBasic, copy);
    }

    @Override
    public QuietlyCloseable subscribeKeyAvailability(KeyConsumer<? super K> keyConsumer) {
        return delegate.subscribeKeyAvailability(keyConsumer);
    }

    @Override
    public void onData(K key, DidoData data) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            delegate.onData(key, data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onPartial(K key, PartialData partial) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            delegate.onPartial(key, partial);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onDelete(K key) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            delegate.onDelete(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <R> R copy(K key, Function<? super DidoData, ? extends R> mapper) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return mapper.apply(delegate.get(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DataSchema getSchema() {
        return copy.getSchema();
    }

    @Override
    public int size() {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return delegate.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return delegate.keySet();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<Map.Entry<K, DidoData>> entrySet() {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            Set<Map.Entry<K, DidoData>> entrySet = new HashSet<>(delegate.size());
            for (Map.Entry<K, DidoData> entry : delegate.entrySet()) {
                entrySet.add(Map.entry(entry.getKey(), copy.apply(entry.getValue())));
            }
            return entrySet;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsKey(K key) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return delegate.containsKey(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DidoData get(K key) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            DidoData data = delegate.get(key);
            if (data == null) {
                return null;
            } else {
                return copy.apply(delegate.get(key));
            }
        } finally {
            lock.unlock();
        }
    }

    protected DidoData doCopy(DidoData data) {
        Lock lock = rwLock.readLock();
        lock.lock();
        try {
            return copy.apply(data);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public DidoSubscription subscribe(KeyedDataConsumer<? super K> consumer) {
        return delegate.subscribe(new KeyedDataConsumer<>() {
            @Override
            public void onData(K key, DidoData data) {
                consumer.onData(key, doCopy(data));
            }

            @Override
            public void onPartial(K key, PartialData partial) {
                consumer.onPartial(key, PartialData.of(
                        doCopy(partial.getData()), partial.getIndices()));
            }

            @Override
            public void onDelete(K key) {
                consumer.onDelete(key);
            }
        });
    }
}
