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
    private static final ObjectMapper OBJECT_MAPPER =
        Jackson.newObjectMapper();
    private static final String DSN =
            "jdbc:mysql://localhost:53306/audit?useUnicode=true&" +
                    "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode" +
                    "=false&serverTimezone=UTC";
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

    public static void main(String...args) throws ClassNotFoundException, JSchException {
        Class.forName("com.mysql.jdbc.Driver");
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");

        JSch jSch = new JSch();
        jSch.addIdentity("/Users/arobinson/.ssh/id_rsa_seo99",
            "/Users/arobinson/.ssh/id_rsa_seo99.pub", null);
        jSch.setKnownHosts("/Users/arobinson/.ssh/known_hosts");

        for (String hostName : CRAWLER_NODES) {
            CallableStatement callableStatement = null;
            Connection connection = null;
            try {
                Session session = jSch.getSession("root", "n3dbmaster", 42724);
                session.setConfig(config);
                session.connect(20000);
                final int localPort = session.setPortForwardingL(53306, hostName, 3306);
                connection = DriverManager.getConnection(DSN,
                    "",
                    "");
                if (null != connection) {
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
                session = null;
            } catch (SQLException | JSchException e) {
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
