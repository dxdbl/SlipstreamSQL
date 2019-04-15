package io.transwarp;

import java.sql.*;

public class demo {
    // Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        // Hive2 jdbc url with LDAP
        String slipstreamServer = "192.168.1.32:10010";
        String database = "default";
        String user = "hive";
        String passwd = "123456";

        String jdbcURL = "jdbc:hive2://" + slipstreamServer + "/" + database;

        // 需要执行的 SQL
        String sql = "drop stream if exists spy_test1;CREATE STREAM spy_test1 (\n" +
                "    MessageNo String\n" +
                "    ,MessageCode STRING\n" +
                "    ,EcCompanyId string\n" +
                "    ,PostalDeparlmentCode string\n" +
                "    ,OperCode INT\n" +
                "    ,StaffCode string\n" +
                "    ,StaffCardType string\n" +
                "    ,StaffCardID string\n" +
                "    ,Name string\n" +
                "    ,Mobile string\n" +
                "    ,StaffStatus string\n" +
                "    ,OutletsName string\n" +
                "    ,OutletsNo string\n" +
                "    ,ProvinceCode string\n" +
                "    ,CityCode string\n" +
                "    ,CountyCode string\n" +
                "    ,Street string\n" +
                "    ) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ','\n" +
                "LINES TERMINATED BY  '\\n'\n" +
                "TBLPROPERTIES(\"topic\"=\"spytest\",\"kafka.zookeeper\"=\"idc27:2181,idc32:2181,idc33:2181\",\"kafka.broker.list\"\n" +
                "=\"idc27:9092,idc32:9092,idc33:9092\");" +
                "drop table if exists spy_hb1;\n" +
                "CREATE TABLE spy_hb1 (\n" +
                "    MessageNo String\n" +
                "    ,MessageCode STRING\n" +
                "    ,EcCompanyId string\n" +
                "    ,PostalDeparlmentCode string\n" +
                "    ,OperCode INT\n" +
                "    ,StaffCode string\n" +
                "    ,StaffCardType string\n" +
                "    ,StaffCardID string\n" +
                "    ,Name string\n" +
                "    ,Mobile string\n" +
                "    ,StaffStatus string\n" +
                "    ,OutletsName string\n" +
                "    ,OutletsNo string\n" +
                "    ,ProvinceCode string\n" +
                "    ,CityCode string\n" +
                "    ,CountyCode string\n" +
                "    ,Street string\n" +
                "    ) STORED AS HYPERDRIVE;" +
                "DROP STREAMJOB IF EXISTS spy;\n" +
                "CREATE STREAMJOB spy AS (\"INSERT INTO spy_hb1 SELECT * FROM spy_test1\" )\n" +
                "jobproperties(\n" +
                "\"streamsql.use.eventmode\"=\"true\",\n" +
                "\"morphling.job.checkpoint.interval\"=\"5000\",\n" +
                "\"morphling.job.enable.checkpoint\"=\"TRUE\",\n" +
                "\"morphling.task.max.failures\"=\"3\");" +
                "START STREAMJOB spy;";


        Connection conn = DriverManager.getConnection(jdbcURL,user,passwd);
        Statement stmt = conn.createStatement();
        long re = stmt.executeUpdate(sql);
//        ResultSet rs = stmt.executeQuery("show tables");

//        ResultSetMetaData rsmd = rs.getMetaData();
//        Integer size = rsmd.getColumnCount();
//        while (rs.next())
//        {
//            StringBuffer value = new StringBuffer();
//            for (int i = 0; i < size; i++) {
//                value.append(rs.getString(i + 1)).append("\t");
//            }
//            System.out.println(value.toString());
//        }

        //rs.close();
        stmt.close();
        conn.close();
    }
}
