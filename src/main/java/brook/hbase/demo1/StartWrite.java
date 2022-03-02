package brook.hbase.demo1;

import brook.hbase.Constant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//@Slf4j
public class StartWrite {

    private static Connection connection;
    private static String zkClientPort = "2181";
    private static List<String> zkQuorumList = Arrays.asList("172.20.3.143","172.20.3.144","172.20.3.145");
    private static String hbMaster = "172.20.3.144:16000";
    private static String zNodeParent = "/hbase";

    private static HbaseTable.ColumnFamily columnFamily1 = HbaseTable.ColumnFamily.builder()
            .columnFamilyName("param")
            .columns(Arrays.asList("id","name"))
            .build();
    private static HbaseTable table1 = HbaseTable.builder()
            .tableName("table_dongwei")
            .columnFamilies(Collections.singletonList(columnFamily1))
            .build();

    private static Map<String, String> map = Stream.of(new String[][] {
            { "rowKey", "first" },
            { "id", "1" },
            { "name", "dongwei" },
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    public static void main(String[] args) throws Exception {
        getConnection();
        writeHbase();

    }

    private static void getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        String zkQuorum = null;
        for (String data : zkQuorumList) {
            if (zkQuorum == null) {
                zkQuorum = data;
                continue;
            }
            zkQuorum = zkQuorum.concat("," + data);
        }
        configuration.set(Constant.ZOOKEEPERCLIENT, zkClientPort);
        configuration.set(Constant.ZOOKEEPERQUORUM, zkQuorum);
        configuration.set(Constant.HBASEMASTER, hbMaster);
        configuration.set(Constant.ZOOKEEPERZNODEPARENT, zNodeParent);
        connection = ConnectionFactory.createConnection(configuration);
    }

    private static void writeHbase() throws Exception {
        Table table = connection.getTable(TableName.valueOf(table1.getTableName()));
        List<Put> putList = new ArrayList<>();

        HbaseTable.ColumnFamily columnFamily = table1.getColumnFamilies().get(0);
            List<String> columns = columnFamily.getColumns();
            for (String column : columns) {
                Put put = new Put(Bytes.toBytes(String.valueOf(map.get("rowKey"))));
                if (map.get(column) == null) {
//                    log.warn("{} doesn't have data", column);
                    continue;
                }
                put.addColumn(Bytes.toBytes(columnFamily.getColumnFamilyName()),
                        Bytes.toBytes(column), Bytes.toBytes(String.valueOf(map.get(column))));
                putList.add(put);
            }
        table.put(putList);
        table.close();

    }

    @ToString
    @AllArgsConstructor
    @Getter
    @Setter
    @Builder
    public static class HbaseTable {
        private String tableName;
        private List<ColumnFamily> columnFamilies;

        @Getter
        @Setter
        @ToString
        @Builder
        public static class ColumnFamily {

            private String columnFamilyName;

            private List<String> columns;
        }
    }
}
