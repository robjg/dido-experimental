package dido.flow.util;

import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingKeyedDataConsumer<K> implements KeyedDataConsumer<K> {

    private static final Logger logger = LoggerFactory.getLogger(LoggingKeyedDataConsumer.class);

    @Override
    public void onData(K key, DidoData data) {
        logger.info("onData: key={}, data={}", key, data);
    }

    @Override
    public void onPartial(K key, PartialData partial) {
        logger.info("onPartial: key={}, data={}", key, partial);
    }

    @Override
    public void onDelete(K key) {
        logger.info("onDelete: key={}", key);
    }
}
