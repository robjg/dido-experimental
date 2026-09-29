package dido.elsewhere.ema;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FromValues;
import dido.data.immutable.NonBoxedDataFactoryProvider;
import dido.data.partial.PartialData;
import dido.flow.KeyedDataEvent;
import dido.table.internal.ConcurrentTableBasic;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DidoIProviderConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(DidoIProviderConsumerTest.class);

    public static final DataSchema SCHEMA = DataSchema.builder()
            .addNamed("BID", double.class)
            .addNamed("ASK", double.class)
            .addNamed("BIDSIZE", int.class)
            .addNamed("ASKSIZE", int.class)
            .build();


    @Test
    void provideAndConsume() throws Exception {

        BlockingQueue<KeyedDataEvent<String>> queue = new LinkedBlockingQueue<>();

        ConcurrentTableBasic<String> table = ConcurrentTableBasic
                .create(SCHEMA, new NonBoxedDataFactoryProvider());

        FromValues fromValues = DidoData.withSchema(SCHEMA);

        DidoData d1 = fromValues.of(99.9, 100.1, 80, 90);
        DidoData d2 = fromValues.of(104.9, 105.1, 30, 20);
        DidoData d3 = fromValues.of(79.9, 80.1, 110, 100);

        PartialData p1 = PartialData.of(fromValues.asBuilder()
                .withDouble("BID", 99.8).build(), 1);
        PartialData p2 = PartialData.of(fromValues.asBuilder()
                .withDouble("ASK", 105.2).build(), 2);


        KeyedDataEvent<String> e1 = KeyedDataEvent.complete("IBM.N", d1);
        KeyedDataEvent<String> e2 = KeyedDataEvent.complete("APPL.OQ", d2);
        KeyedDataEvent<String> e3 = KeyedDataEvent.complete("MSFT.OQ", d3);

        KeyedDataEvent<String> e4 = KeyedDataEvent.partial("IBM.N", p1);
        KeyedDataEvent<String> e5 = KeyedDataEvent.partial("APPL.OQ", p2);


        Queue<KeyedDataEvent<String>> expected = new LinkedList<>(List.of(e1, e2, e3, e4, e5));

        int port = freePort();

        try (AutoCloseable providerClose = DidoOmmIProvider.with()
                .port(port)
                .from(table);

             AutoCloseable consumerClose = DidoOmmConsumer.with()
                     .host("localhost:" + port)
                     .serviceName("DIRECT_FEED")
                     .schema(SCHEMA)
                     .keyAvailability(table)
                     .to(KeyedDataEvent.asKeyedDataConsumer(queue::add))) {

            logger.info("** Started Test");

            List.of(e1, e2, e3)
                    .forEach(KeyedDataEvent.fromKeyedDataConsumer(table));

            logger.info("** Table Populated");

            for (int i = 0; i < 3; i++) {

                KeyedDataEvent<String> event = queue.poll(5, TimeUnit.SECONDS);

                System.out.println(event);

                MatcherAssert.assertThat(event, Matchers.equalTo(expected.remove()));
            }

            List.of(e4, e5)
                    .forEach(KeyedDataEvent.fromKeyedDataConsumer(table));

            logger.info("** Events sent");

            for (int i = 0; i < 2; i++) {

                KeyedDataEvent<String> event = queue.poll(5, TimeUnit.SECONDS);

                System.out.println(event);

                MatcherAssert.assertThat(event, Matchers.equalTo(expected.remove()));
            }
        }

    }

    int freePort() throws IOException {
        try (
                ServerSocket socket = new ServerSocket(0);
        ) {
            return socket.getLocalPort();
        }
    }


}

