package dido.table.ui;

import dido.data.DataSchema;
import dido.data.DidoData;
import dido.data.partial.PartialData;
import dido.flow.DidoSubscription;
import dido.flow.KeyedDataConsumer;
import dido.flow.SubscriptionOptions;
import dido.table.DataTable;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTableView<K> {

    private final DataTable<K> dataTable;
    private final JFrame frame;
    private final JTable table;
    private final DataTableTableModel tableModel;
    private DidoSubscription subscription;

    public DataTableView(DataTable<K> dataTable) {
        this.dataTable = dataTable;
        this.tableModel = new DataTableTableModel();
        this.table = new JTable(tableModel);
        this.frame = new JFrame("DataTable: " + dataTable.getSchema());

        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.add(new JScrollPane(table));
        frame.pack();
    }

    public void show() {
        subscribe();
        frame.setVisible(true);
    }

    public JFrame getFrame() {
        return frame;
    }

    public JTable getTable() {
        return table;
    }

    public void dispose() {
        unsubscribe();
        frame.dispose();
    }

    private void subscribe() {
        unsubscribe();
        subscription = dataTable.subscribeKeyedData(new TableUpdateConsumer(), SubscriptionOptions.of().all());
    }

    private void unsubscribe() {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
    }

    private class DataTableTableModel extends AbstractTableModel {

        private final List<Integer> columnIndices;
        private final Map<Integer, Integer> schemaToTableColumn;

        private List<K> keys = new ArrayList<>();
        private Map<K, Integer> keyToRow = new HashMap<>();

        DataTableTableModel() {
            DataSchema schema = dataTable.getSchema();
            columnIndices = new ArrayList<>();
            schemaToTableColumn = new HashMap<>();
            for (int i = 0, index = schema.firstIndex(); index > 0; i++, index = schema.nextIndex(index)) {
                columnIndices.add(index);
                schemaToTableColumn.put(index, i);
            }
        }

        void refresh() {
            keys = new ArrayList<>(dataTable.keySet());
            keyToRow.clear();
            for (int i = 0; i < keys.size(); i++) {
                keyToRow.put(keys.get(i), i);
            }
            fireTableDataChanged();
        }

        @Override
        public int getRowCount() {
            return keys.size();
        }

        @Override
        public int getColumnCount() {
            return columnIndices.size();
        }

        @Override
        public String getColumnName(int columnIndex) {
            int schemaIndex = columnIndices.get(columnIndex);
            return dataTable.getSchema().getFieldNameAt(schemaIndex);
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            K key = keys.get(rowIndex);
            DidoData data = dataTable.get(key);
            int schemaIndex = columnIndices.get(columnIndex);
            return data.getAt(schemaIndex);
        }
    }

    private class TableUpdateConsumer implements KeyedDataConsumer<K> {

        @Override
        public void snapshotComplete() {
            SwingUtilities.invokeLater(() -> tableModel.refresh());
        }

        @Override
        public void onData(K key, DidoData data) {
            SwingUtilities.invokeLater(() -> {
                DataTableTableModel model = tableModel;
                Map<K, Integer> keyToRow = model.keyToRow;
                List<K> keys = model.keys;

                Integer existingRow = keyToRow.get(key);
                if (existingRow != null) {
                    model.fireTableRowsUpdated(existingRow, existingRow);
                } else {
                    int row = keys.size();
                    keys.add(key);
                    keyToRow.put(key, row);
                    model.fireTableRowsInserted(row, row);
                }
            });
        }

        @Override
        public void onPartial(K key, PartialData partial) {
            SwingUtilities.invokeLater(() -> {
                DataTableTableModel model = tableModel;
                Map<K, Integer> keyToRow = model.keyToRow;
                Map<Integer, Integer> schemaToTableColumn = model.schemaToTableColumn;

                Integer rowIndex = keyToRow.get(key);
                if (rowIndex == null) {
                    return;
                }

                for (int schemaIndex = partial.firstIndex(); schemaIndex > 0; schemaIndex = partial.nextIndex(schemaIndex)) {
                    Integer colIndex = schemaToTableColumn.get(schemaIndex);
                    if (colIndex != null) {
                        model.fireTableCellUpdated(rowIndex, colIndex);
                    }
                }
            });
        }

        @Override
        public void onDelete(K key) {
            SwingUtilities.invokeLater(() -> {
                DataTableTableModel model = tableModel;
                Map<K, Integer> keyToRow = model.keyToRow;
                List<K> keys = model.keys;

                Integer rowIndex = keyToRow.remove(key);
                if (rowIndex != null) {
                    keys.remove(rowIndex.intValue());
                    for (int i = rowIndex; i < keys.size(); i++) {
                        keyToRow.put(keys.get(i), i);
                    }
                    model.fireTableRowsDeleted(rowIndex, rowIndex);
                }
            });
        }
    }
}
