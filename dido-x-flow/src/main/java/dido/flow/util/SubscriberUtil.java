package dido.flow.util;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.*;

import java.util.function.Function;

public class SubscriberUtil {


    public static <K> DidoSubscriber didoSubscriberFrom(KeyedDidoSubscriber<K> keyedDataSubscriber,
                                                        DataSchema schema) {
        return didoSubscriberFrom(keyedDataSubscriber,
                KeyUtil.fromFirstField(schema));
    }

    public static <K> DidoSubscriber didoSubscriberFrom(KeyedDidoSubscriber<K> keyedDataSubscriber,
                                                        Function<? super DidoData, ? extends K> keyExtractor) {

        return new DidoSubscriber() {
            @Override
            public void onData(DidoData data) {

                K key = keyExtractor.apply(data);

                keyedDataSubscriber.onData(key, data);
            }

            @Override
            public void onPartial(PartialData partial) {

                DidoData data = partial.getData();

                K key = keyExtractor.apply(data);

                keyedDataSubscriber.onPartial(key, partial);
            }

            @Override
            public void onDelete(DidoData keyData) {

                K key = keyExtractor.apply(keyData);

                keyedDataSubscriber.onDelete(key);
            }
        };
    }

    public static <K> DidoPublisher didoPublisherFrom(KeyedDidoPublisher<K> keyedPublisher,
                                                      Function<? super K, ? extends DidoData> keyComposer) {

        return new DidoPublisher() {

            @Override
            public DidoSubscription subscribe(DidoSubscriber subscriber) {

                KeyedDidoSubscriber<K> keyedSubscriber = new KeyedDidoSubscriber<K>() {
                    @Override
                    public void onData(K key, DidoData data) {
                        subscriber.onData(data);
                    }

                    @Override
                    public void onPartial(K key, PartialData partial) {
                        subscriber.onPartial(partial);
                    }

                    @Override
                    public void onDelete(K key) {
                        subscriber.onDelete(keyComposer.apply(key));
                    }
                };

                return keyedPublisher.subscribe(keyedSubscriber);
            }
        };
    }

}
