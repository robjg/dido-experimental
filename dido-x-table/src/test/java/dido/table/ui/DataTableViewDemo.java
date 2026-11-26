package dido.table.ui;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.FromValues;
import dido.flow.DidoDataConsumer;
import dido.flow.util.SubscriberUtil;
import dido.table.internal.DataTableBasic;

import java.util.ArrayList;
import java.util.List;

public class DataTableViewDemo {

    public static void main(String[] args) {

        DataSchema fruitSchema = DataSchema.builder()
                .addNamed("Id", String.class)
                .addNamed("Fruit", String.class)
                .addNamed("GrocerId", String.class)
                .addNamed("Price", double.class)
                .build();

        FromValues fromValues = DidoData.withSchema(fruitSchema);
        List<DidoData> fruit = new ArrayList<>();
        for (int i = 0; i < 100; ) {
            fruit.add(fromValues.of("F" + ++i, "Apple", "G2", 5));
            fruit.add(fromValues.of("F" + ++i, "Banana", "G2", 3));
            fruit.add(fromValues.of("F" + ++i, "Orange", "G1", 2));
        }

        DataTableBasic<String> fruitTable = DataTableBasic.forSchema(fruitSchema);

        DidoDataConsumer fruitSubscriber  = SubscriberUtil.didoSubscriberFrom(
                fruitTable, fruitSchema);

        fruit.forEach(fruitSubscriber::onData);

        DataTableView<String> dtv = new DataTableView<>(fruitTable);

        dtv.show();
    }
}
