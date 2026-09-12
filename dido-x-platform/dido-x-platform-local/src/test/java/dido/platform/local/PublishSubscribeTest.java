package dido.platform.local;


import dido.data.DataSchema;
import dido.data.DidoData;
import dido.platform.Platform;
import dido.platform.Receiver;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class PublishSubscribeTest {

    DataSchema schema = DataSchema.builder()
            .addNamed("Fruit", String.class)
            .addNamed("Price", double.class)
            .build();

    @Test
    void simplePublishQuery() {

        Platform platform = LocalPlatform.create();

        Receiver receiver = platform.send().endpoint("FRUIT")
                .schema(schema)
                .start();

        DidoData data = DidoData.withSchema(schema)
                .of("Apple", 23.4);

        receiver.onData(data);

        receiver.close();

        List<DidoData> results = platform.query()
                .endpoint("FRUIT")
                .stream()
                .toList();

        assertThat(results, contains(data));
    }

}