package com.searchmetrics.mysql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.dropwizard.jackson.Jackson;

import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;
/**
 *
 */
public class CrawlerJobsInfo {
    private static final String SSH_CONFIG_DIR = String.join("/", System.getProperty("user.home") + ".ssh");
    private static final String KNOWN_HOSTS = String.join("/", SSH_CONFIG_DIR + "known_hosts");
    private static final String PRIVATE_KEY = String.join("/", SSH_CONFIG_DIR + "id_rsa_seo99");
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
        new HashSet<>(Arrays.asList(22, 26, 27));
    private static final List<String> CRAWLER_NODES = new ArrayList<String>(){
        {
            for (int i : IntStream.range(1, 33).toArray()) {
                if (! NON_PROD_CRAWLER_NODES.contains(i))
                    add(String.format("n3ac0%02d", i));
            }
        }
    };

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");

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
    }

    public static Session tunnelToHost(final JSch channel, final String host) throws JSchException {
        Session session = channel.getSession("root", "n3dbmaster", 42724);
        session.setConfig(SESSION_CONFIG);
        session.connect(20000);
        final int localPort = session.setPortForwardingL(53306, host, 3306);

        return session;
    }

    public static void main(String...args) throws JSchException {

        for (String hostName : CRAWLER_NODES) {
            CallableStatement callableStatement = null;
            Connection connection = null;

            try {

                Session session = tunnelToHost(SECURE_CHANNEL, hostName);

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
                    callableStatement = connection.prepareCall("select * from crawler_status");

                    if (null != callableStatement) {
                        ResultSet resultSet = callableStatement.executeQuery();
                        if (null != resultSet) {
                            System.out.println("-- " + hostName);
                            while (resultSet.next()) {
                                System.out.println( resultSet.getString("act_audit_id") + ",");
                            }
                        }
                    }
                }

                session.disconnect();

            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally {

                try {
                    callableStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
