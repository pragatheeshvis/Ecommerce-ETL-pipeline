package com.ecommerce.etl.config;

import java.io.Serializable;

/**
 * Centralized configuration for MySQL database connectivity.
 * Modify the constants below to match your MySQL environment.
 */
public class DatabaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Connection Settings ─────────────────────────────────────
    private static final String HOST     = "localhost";
    private static final int    PORT     = 3306;
    private static final String DATABASE = "ecommerce_etl";
    private static final String USER     = "root";
    private static final String PASSWORD = "root";  // Change to your MySQL password

    // ── JDBC URL ────────────────────────────────────────────────
    private static final String JDBC_URL = String.format(
        "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
        HOST, PORT, DATABASE
    );

    // ── JDBC Driver ─────────────────────────────────────────────
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    // ── Spark JDBC Write Batch Size ─────────────────────────────
    private static final int BATCH_SIZE = 1000;

    // ── Getters ─────────────────────────────────────────────────

    public static String getJdbcUrl() {
        return JDBC_URL;
    }

    public static String getUser() {
        return USER;
    }

    public static String getPassword() {
        return PASSWORD;
    }

    public static String getJdbcDriver() {
        return JDBC_DRIVER;
    }

    public static String getDatabase() {
        return DATABASE;
    }

    public static int getBatchSize() {
        return BATCH_SIZE;
    }

    /**
     * Returns a java.util.Properties object pre-filled with JDBC credentials,
     * ready for Spark DataFrame .jdbc() calls.
     */
    public static java.util.Properties getConnectionProperties() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user",   USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("driver", JDBC_DRIVER);
        props.setProperty("batchsize", String.valueOf(BATCH_SIZE));
        props.setProperty("rewriteBatchedStatements", "true");
        return props;
    }

    @Override
    public String toString() {
        return String.format("DatabaseConfig[url=%s, user=%s, db=%s]", JDBC_URL, USER, DATABASE);
    }
}

