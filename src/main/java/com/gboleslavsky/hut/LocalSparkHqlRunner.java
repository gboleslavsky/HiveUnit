package com.gboleslavsky.hut;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class LocalSparkHqlRunner {

    private LocalSparkHqlRunner() {}

    private static String warehouseLocation = System.getProperty("user.dir") + "/spark_warehouse";

    public static SparkSession hive = SparkSession
            .builder()
            .appName("Hive Test Tools")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .config("spark.master", "local")
            .enableHiveSupport()
            .getOrCreate();


    public static void runStatement(String statement) { hive.sql(statement);}

    public static Dataset<Row> runStatementReturnResults(String statement) { return hive.sql(statement);}

    public static void runScript( String script) {
        String[] statements = splitScriptIntoStatements(script);
        for (String statement : statements) { hive.sql(statement);}
    }

    public static Dataset<Row> runScriptReturnResults(String script) {
        String[] statements = splitScriptIntoStatements(script);
        Dataset<Row> ds = hive.sql(statements[statements.length - 1]);
        for (int i = 0; i < statements.length - 1; i++) { ds = ds.union(hive.sql(statements[i])); }
        return ds.cache();
    }

    public static String[] splitScriptIntoStatements(String script) {
        String scriptNoExtraLines = script.replaceAll("\n(\n|\r\n)+", "\n");
        String scriptNoComments = scriptNoExtraLines.replaceAll("--.*(\n|\r\n|$)", "");
        return scriptNoComments.split(";\n|;\r\n|;$");
    }
}
