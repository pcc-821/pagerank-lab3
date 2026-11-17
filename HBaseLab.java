import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLab {
    private final Connection connection;
    private final Admin admin;
    private final Scanner input;

    public HBaseLab(Connection connection, Admin admin, Scanner input) {
        this.connection = connection;
        this.admin = admin;
        this.input = input;
    }

    public void createTable(String tableName, String[] fields) throws IOException {
        if (fields.length == 0) {
            throw new IllegalArgumentException("At least one column family is required");
        }
        TableName name = TableName.valueOf(tableName);
        Set<String> families = new LinkedHashSet<>();
        for (String field : fields) {
            families.add(field.split(":", 2)[0]);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name);
        for (String family : families) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
        }
        TableDescriptor descriptor = builder.build();

        if (admin.tableExists(name)) {
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            }
            admin.deleteTable(name);
        }
        admin.createTable(descriptor);
    }

    public void addRecord(String tableName, String row, String[] fields,
                          String[] values) throws IOException {
        if (fields.length == 0 || fields.length != values.length) {
            throw new IllegalArgumentException("Supply one value for each field");
        }
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] parts = fields[i].split(":", 2);
            String qualifier = parts.length == 2 ? parts[1] : "";
            put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(qualifier),
                          Bytes.toBytes(values[i]));
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        }
    }

    public Map<String, Map<String, String>> scanColumn(String tableName, String column)
            throws IOException {
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        // Scan each row so a missing column can be returned as null.
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner results = table.getScanner(new Scan())) {
            for (Result result : results) {
                Map<String, String> cells = new LinkedHashMap<>();
                for (Cell cell : result.rawCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String field = family + ":" + qualifier;
                    if (column == null || column.equals(family) || column.equals(field)) {
                        cells.put(field, Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                rows.put(Bytes.toString(result.getRow()), cells.isEmpty() ? null : cells);
            }
        }
        return rows;
    }

    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.println("New value for " + tableName + "/" + row + "/" + column + ":");
        addRecord(tableName, row, new String[]{column}, new String[]{input.nextLine()});
    }

    public void deleteRow(String tableName, String row) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)));
        }
    }

    public long countRows(String tableName) throws IOException {
        long count = 0;
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner results = table.getScanner(new Scan().setFilter(new FirstKeyOnlyFilter()))) {
            for (Result ignored : results) {
                count++;
            }
        }
        return count;
    }

    public static void main(String[] args) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
             Admin admin = connection.getAdmin();
             Scanner input = new Scanner(System.in, "UTF-8")) {
            HBaseLab lab = new HBaseLab(connection, admin, input);

            System.out.println("Tables: " + java.util.Arrays.toString(admin.listTableNames()));
            for (String name : new String[]{"Student", "Course", "SC"}) {
                System.out.println(name + " rows: " + lab.countRows(name));
                lab.scanColumn(name, null)
                   .forEach((row, cells) -> System.out.println(row + " -> " + cells));
            }
            System.out.println("SC Score family: " + lab.scanColumn("SC", "Score"));
            System.out.println("SC Score:Math: " + lab.scanColumn("SC", "Score:Math"));

            String demoName = "lab3_java_demo";
            TableName demo = TableName.valueOf(demoName);
            String[] fields = {"Info:Name", "Score:Math"};
            lab.createTable(demoName, fields);
            lab.addRecord(demoName, "A", fields, new String[]{"Alice", "86"});
            lab.modifyData(demoName, "A", "Score:Math");
            System.out.println("After update: " + lab.scanColumn(demoName, "Score:Math"));

            admin.addColumnFamily(demo, ColumnFamilyDescriptorBuilder.of("Temp"));
            lab.addRecord(demoName, "A", new String[]{"Temp:note"}, new String[]{"example"});
            System.out.println("Before column delete: " + lab.scanColumn(demoName, "Temp:note"));
            try (Table table = connection.getTable(demo)) {
                table.delete(new Delete(Bytes.toBytes("A"))
                    .addColumns(Bytes.toBytes("Temp"), Bytes.toBytes("note")));
            }
            System.out.println("After column delete: " + lab.scanColumn(demoName, "Temp:note"));
            admin.deleteColumnFamily(demo, Bytes.toBytes("Temp"));
            System.out.println("Temp family exists: "
                + admin.getDescriptor(demo).hasColumnFamily(Bytes.toBytes("Temp")));

            lab.addRecord(demoName, "B", fields, new String[]{"Bob", "90"});
            lab.deleteRow(demoName, "B");
            System.out.println("After row delete: " + lab.countRows(demoName));

            admin.disableTable(demo);
            admin.truncateTable(demo, true);
            System.out.println("After truncate: " + lab.countRows(demoName));

            lab.addRecord(demoName, "A", fields, new String[]{"Alice", "89"});
            lab.createTable(demoName, fields);
            System.out.println("After recreate: " + lab.countRows(demoName));
        }
    }
}
