package com.netcloud.bigdata.flink.sql._01_basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;

/**
 * @author netcloud
 * @date 2023-05-15 11:37:12
 * @email netcloudtec@163.com
 * @description 方法一: 通过EnvironmentSettings创建TableEnvironment
 */
public class GettingStartedExample {
    public static void main(String[] args) throws Exception {

        // 设置统一API
        // 在这种场景下:声明表程序应该以批处理模式执行
        final EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() //声明为流任务
//                .inBatchMode()     //声明批流任务
                .build();
        final TableEnvironment env = TableEnvironment.create(settings);

        //创建一个表，用示例数据而不是用连接器
        final Table rawCustomers =
                env.fromValues(
                        Row.of(
                                "Guillermo Smith",
                                LocalDate.parse("1992-12-12"),
                                "4081 Valley Road",
                                "08540",
                                "New Jersey",
                                "m",
                                true,
                                0,
                                78,
                                3),
                        Row.of(
                                "Valeria Mendoza",
                                LocalDate.parse("1970-03-28"),
                                "1239  Rainbow Road",
                                "90017",
                                "Los Angeles",
                                "f",
                                true,
                                9,
                                39,
                                0),
                        Row.of(
                                "Leann Holloway",
                                LocalDate.parse("1989-05-21"),
                                "2359 New Street",
                                "97401",
                                "Eugene",
                                null,
                                true,
                                null,
                                null,
                                null),
                        Row.of(
                                "Brandy Sanders",
                                LocalDate.parse("1956-05-26"),
                                "4891 Walkers-Ridge-Way",
                                "73119",
                                "Oklahoma City",
                                "m",
                                false,
                                9,
                                39,
                                0),
                        Row.of(
                                "John Turner",
                                LocalDate.parse("1982-10-02"),
                                "2359 New Street",
                                "60605",
                                "Chicago",
                                "m",
                                true,
                                12,
                                39,
                                0),
                        Row.of(
                                "Ellen Ortega",
                                LocalDate.parse("1985-06-18"),
                                "2448 Rodney STreet",
                                "85023",
                                "Phoenix",
                                "f",
                                true,
                                0,
                                78,
                                3));

        //处理列的范围
        final Table truncatedCustomers = rawCustomers.select(withColumns(range(1, 7)));

        // 命名列
        final Table namedCustomers =
                truncatedCustomers.as(
                        "name",
                        "date_of_birth",
                        "street",
                        "zip_code",
                        "city",
                        "gender",
                        "has_newsletter");

        //注册临时视图
        env.createTemporaryView("customers", namedCustomers);

        // 使用SQL
        // 调用execute()和print()结果展示
        env.sqlQuery("SELECT "
                        + "  COUNT(*) AS `number of customers`, "
                        + "  AVG(YEAR(date_of_birth)) AS `average birth year` "
                        + "FROM `customers`"
                ).execute().print();

        // 使用TableAPI进行数据转换
        final Table youngCustomers =
                env.from("customers")
                        .filter($("gender").isNotNull())
                        .filter($("has_newsletter").isEqual(true))
                        .filter($("date_of_birth").isGreaterOrEqual(LocalDate.parse("1980-01-01")))
                        .select(
                                $("name").upperCase(),
                                $("date_of_birth"),
                                call(AddressNormalizer.class, $("street"), $("zip_code"), $("city"))
                                        .as("address"));
        // 使用execute()和collect()从集群中收集结果
        // 这对于在将其存储到外部系统之前进行测试非常有用
        try (CloseableIterator<Row> iterator = youngCustomers.execute().collect()) {
            final Set<Row> expectedOutput = new HashSet<>();
            expectedOutput.add(
                    Row.of(
                            "GUILLERMO SMITH",
                            LocalDate.parse("1992-12-12"),
                            "4081 VALLEY ROAD, 08540, NEW JERSEY"));
            expectedOutput.add(
                    Row.of(
                            "JOHN TURNER",
                            LocalDate.parse("1982-10-02"),
                            "2359 NEW STREET, 60605, CHICAGO"));
            expectedOutput.add(
                    Row.of(
                            "ELLEN ORTEGA",
                            LocalDate.parse("1985-06-18"),
                            "2448 RODNEY STREET, 85023, PHOENIX"));

            final Set<Row> actualOutput = new HashSet<>();
            iterator.forEachRemaining(actualOutput::add);

            if (actualOutput.equals(expectedOutput)) {
                System.out.println("SUCCESS!");
            } else {
                System.out.println("FAILURE!");
            }
        }
    }

    /**
     * 用户自定义函数
     **/
    public static class AddressNormalizer extends ScalarFunction {

        public String eval(String street, String zipCode, String city) {
            return normalize(street) + ", " + normalize(zipCode) + ", " + normalize(city);
        }

        private String normalize(String s) {
            return s.toUpperCase().replaceAll("\\W", " ").replaceAll("\\s+", " ").trim();
        }
    }
}
