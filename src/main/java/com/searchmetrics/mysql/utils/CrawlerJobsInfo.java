package com.searchmetrics.mysql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.dropwizard.jackson.Jackson;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class CrawlerJobsInfo {
    private static final String SSH_CONFIG_DIR = String.join("/", System.getProperty("user.home"), ".ssh");
    private static final String KNOWN_HOSTS = String.join("/", SSH_CONFIG_DIR, "known_hosts");
    private static final String PRIVATE_KEY = String.join("/", SSH_CONFIG_DIR, "id_rsa_seo99");
    private static final String PUBLIC_KEY = PRIVATE_KEY + ".pub";

    private static final String LOCAL_DB_URL =
        "jdbc:mysql://localhost:53306/audit?useUnicode=true&" +
            "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode" +
            "=false&serverTimezone=UTC";


    private static final JSch SECURE_CHANNEL = new JSch();
    private static final Properties SESSION_CONFIG = new Properties();
    private static final ObjectMapper OBJECT_MAPPER =
        Jackson.newObjectMapper();
    private static final Set<Integer> NON_PROD_CRAWLER_NODES =
        new HashSet<>(Arrays.asList(22, 25, 26, 27));
    private static final List<String> CRAWLER_NODES = new ArrayList<String>() {
        {
            for (int i : IntStream.range(1, 34).toArray()) {
                if (! NON_PROD_CRAWLER_NODES.contains(i))
                    add(String.format("n3ac0%02d", i));
            }
        }
    };

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            SECURE_CHANNEL.addIdentity(PRIVATE_KEY, PUBLIC_KEY, null);
            SECURE_CHANNEL.setKnownHosts(KNOWN_HOSTS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Can't load MySQL driver: " + e.getMessage());
            System.exit(1);
        } catch (JSchException e) {
            e.printStackTrace();
            System.out.println("Error configuring JSch object: " + e.getMessage());
            System.exit(1);
        }

        SESSION_CONFIG.put("StrictHostKeyChecking", "no");
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(LOCAL_DB_URL,
            "",
            "");
//            System.getenv("MYSQL_USER"),
//            System.getenv("MYSQL_PASS"));
    }

    public static Session tunnelToHost(final JSch channel, final String host) throws JSchException {
        Session session = channel.getSession("root", "n3dbmaster", 42724);
        session.setConfig(SESSION_CONFIG);
        session.connect(20000);
        final int localPort = session.setPortForwardingL(53306, host, 3306);

        return session;
    }

    public static void main(String...args) {
        try {
            main1(args);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }
    public static void main1(String... args) throws JSchException {

        for (String hostName : CRAWLER_NODES) {
            CallableStatement callableStatement = null;
            Connection connection = null;
            Session session = null;

            try {

                session = tunnelToHost(SECURE_CHANNEL, hostName);

                connection = getConnection();

                if (null != connection) {
//                    DSL.using(connection)
//                        .fetch("SELECT * FROM crawler_status")
//                        .stream()
//                        .collect(groupingBy(
//                            r -> r.getValue("TABLE_NAME"),
//                            mapping(
//                                r -> r.getValue("COLUMN_NAME"),
//                                toList()
//                            )
//                        ))
//                        .forEach(
//                            (table, columns) -> {
//                            }
//                        );
                    callableStatement = connection.prepareCall("SELECT * FROM crawler_status");

                    if (null != callableStatement) {
                        ResultSet resultSet = callableStatement.executeQuery();
                        if (null != resultSet) {
                            System.out.println("-- " + hostName);
                            while (resultSet.next()) {
                                System.out.println(resultSet.getString("act_audit_id") + ",");
                            }
                        }
                    }
                }

                session.disconnect();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {

                if (null != session)
                    session.disconnect();

                try {
                    if (null != callableStatement)
                        callableStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    if (null != connection)
                        connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void updateStatus() {
        try {
            updateJobStatus(Arrays.asList("/Users/adamrobinson/git/cassandra-utils/new.csv"), "new");
            updateJobStatus(Arrays.asList("/Users/adamrobinson/git/cassandra-utils/proc.csv"), "proc");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    enum Status {NEW, PROC;}

    public static void updateJobStatus(final List<String> filenames, final String status)
        throws IOException, JSchException, SQLException {
//        PreparedStatement preparedStatement;
        Connection connection;

        Session session = tunnelToHost(SECURE_CHANNEL, "n3audit03");

        connection = getConnection();

        if (null != connection) {

            for (String filename : filenames) {

                List<Long> contents =
                    java.nio.file.Files.readAllLines(
                        Paths.get(filename),
                        Charset.forName("UTF-8"))
                        .stream()
////                        .filter(m -> m.matches("^(?:new|proc)$"))
                        .map(s -> Long.valueOf(s))
                        .collect(Collectors.toList());

                Iterables.partition(contents, 1000).forEach(
                    b -> {

//                        String batch = String.join(",", b);
                        System.out.println("List size: " + b.size());
                        try {
                            final PreparedStatement preparedStatement = connection.prepareStatement(
                                "UPDATE jobs SET status='" + Status.valueOf(status.toUpperCase()).name().toLowerCase() +
                                    "' WHERE id IN (" +
                                    String.join(",", Collections.nCopies(b.size(), "?")) + "  )"
                            );

                            for (int j = 0; j < b.size(); j++) {
                                preparedStatement.setLong((j+1), b.get(j));
                            }
//                            preparedStatement.setString(1, status);
//                            preparedStatement.setString(2, batch);
                            boolean success = preparedStatement.execute();
                            int updateCount = preparedStatement.getUpdateCount();
                            System.out.println("Updated [" + updateCount + "] records...");
                            preparedStatement.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                );
            }
//            preparedStatement.close();
        }

        connection.close();
        session.disconnect();
    }
}
