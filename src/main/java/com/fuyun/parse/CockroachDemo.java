package com.fuyun.parse;

import static jdk.nashorn.internal.objects.Global.print;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CockroachDemo {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        simpleDemo();
        wrongDemo();
    }

    private static void wrongDemo() throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
            DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        Class.forName("org.postgresql.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:postgresql://172.19.79.8:26257/baize?useUnicode=true&sslmode=require&characterEncoding=UTF-8");
        dataSource.setUsername("fuyun");
        dataSource.setPassword("fuyun2019");

        Schema schema = JdbcSchema.create(rootSchema, null, dataSource,
                                          null, null);

        rootSchema.add("public", schema);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
            "select u.id from public.users u"
        );
        print(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    private static void simpleDemo() throws SQLException {
        Connection connection =
            DriverManager.getConnection("jdbc:postgresql://172.19.79.8:26257/baize?useUnicode=true&sslmode=require&characterEncoding=UTF-8",
                                        "fuyun", "fuyun2019");
        ResultSet result = connection.getMetaData().getTables( null, null, null, null);
        while( result.next()) {
            System. out.println( "Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result .getString(3));
        }
        result.close();

        Statement st = connection.createStatement();
        result = st.executeQuery("select * from public.users u");
        while( result.next()) {
            System. out.println( result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3));
        }
        result.close();
        connection.close();
    }
}
