package com.roy.flink.table;

import com.roy.flink.beans.Stock;
import com.roy.flink.streaming.FileRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author roy
 * @date 2021/9/12
 * @desc
 */
public class FileTableDemo {
    public static void main(String[] args) throws Exception {
        //1、读取数据
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final URL resource = FileRead.class.getResource("/stock.txt");
        final String filePath = resource.getFile();
//        final DataStreamSource<String> stream = env.readTextFile(filePath);
        final DataStreamSource<String> dataStream = env.readFile(new TextInputFormat(new Path(filePath)), filePath);
        final SingleOutputStreamOperator<Stock> stockStream = dataStream
                .map((MapFunction<String, Stock>) value -> {
                    final String[] split = value.split(",");
                    return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
                });
        //2、创建StreamTableEnvironment catalog -> database -> tablename
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
//        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3、基于流创建表
        final Table stockTable = tableEnv.fromDataStream(stockStream);

        final Table table = stockTable.groupBy($("id"), $("stockName"))
                .select($("id"), $("stockName"), $("price").avg().as("priceavg"))
                .where($("stockName").isEqual("UDFStock"));
        //转换成流
        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> tableDataStream =
                tableEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
        }));
        tableDataStream.print("table");
        //4、使用SQL查询
//        tableEnv.createTemporaryView("stock",stockTable);
//        String sql = "select id,stockName,avg(price) as priceavg from stock where stockName='UDFStock' group by id,stockName";
//        final Table sqlTable = tableEnv.sqlQuery(sql);
//        //转换成流
//        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream =
//                tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {}));
//        sqlTableDataStream.print("sql");


        //这种建表方式为什么不行？插入时总是报表不存在。。
//        TableSchema schema = TableSchema.builder()
//                .field("id", DataTypes.STRING())
//                .field("stockName", DataTypes.STRING())
//                .field("price", DataTypes.DOUBLE())
//                .field("`timestamp`", DataTypes.TIMESTAMP())
//                .build();
//
//        Map<String,String> properties = new HashMap<>();
//        properties.put("connector.type","filesystem");
//        properties.put("format.type","csv");
//        properties.put("connector.path","D://flinktest");
//        final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog("default_catalog", "default_database");
//        catalog.createTable(new ObjectPath("default_database","stock")
//                , new CatalogTableImpl(
//                        schema,
//                        properties,
//                        "comment"
//                )
//                ,false);
//        final List<String> databases = catalog.listDatabases();
//        databases.forEach(System.out::println);
//        final List<String> default_database = catalog.listTables("default_database");
//        default_database.forEach(System.out::println);
//        final CatalogBaseTable stockCata = catalog.getTable(new ObjectPath("default_database","stock"));
//        System.out.println(stockCata.getOptions());
//
//        final Table table = tableEnv.fromDataStream(stockStream);
//        table.executeInsert("stock");
//        String sql = "select id,stockName,avg(price) as priceavg from stock where stockName='UDFStock' group by id,stockName";
//        final Table sqlTable = tableEnv.sqlQuery(sql);
//        //转换成流
//        final DataStream<Tuple2<Boolean, Tuple3<String, String, Double>>> sqlTableDataStream = tableEnv.toRetractStream(sqlTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
//        }));
//        sqlTableDataStream.print("sql");

        env.execute("FileTableDemo");
    }
}
