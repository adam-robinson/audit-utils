package com.searchmetrics.mysql;

import com.google.common.collect.ImmutableMap;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
@PropertySource(value = "classpath:application.properties")
public class Utils {
    public static final Utils INSTANCE = new Utils();

    private static final String SSH_CONFIG_DIR = String.join("/", System.getProperty("user.home"), ".ssh");
    private static final String KNOWN_HOSTS = String.join("/", SSH_CONFIG_DIR, "known_hosts");
    private static final String PRIVATE_KEY = String.join("/", SSH_CONFIG_DIR, "id_rsa_seo99");
    private static final String PUBLIC_KEY = PRIVATE_KEY + ".pub";

    private static final String CONNECTION_URL_FORMAT =
        "jdbc:mysql://localhost:%d/audit?useUnicode=true&" +
            "useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode" +
            "=false&serverTimezone=UTC";

    private static final JSch SECURE_CHANNEL = new JSch();

    private static final Integer STARTING_TUNNEL_PORT = 13307;

    private static final String TUNNEL_HOST = "n3dbmaster";
    private static final String TUNNEL_USER = "root";
    private static final Integer TUNNEL_PORT = 42724;

    private static Integer lastTunnelPort = STARTING_TUNNEL_PORT;

    static {
        try {
            SECURE_CHANNEL.addIdentity(PRIVATE_KEY, PUBLIC_KEY, null);
            SECURE_CHANNEL.setKnownHosts(KNOWN_HOSTS);
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }


    private final Map<String, TunneledConnection> TUNNELED_CONNECTION_MAP =
        new HashMap<>(5);

    @Value("${mysql_user}")
    private String mysqlUser;

    @Value("${mysql_pass}")
    private String mysqlPass;

    private Properties mysqlConnectionProperties = new Properties() {{
        ImmutableMap.builder().put("user", mysqlUser).put( "password", mysqlPass).build();
    }};

    private Utils(){

    }

    public TunneledConnection getTunnelViaN3DBMaster( final String hostName ) throws JSchException, SQLException {
        if (! TUNNELED_CONNECTION_MAP.containsKey(hostName)) {

            final Integer tunnelPort = getNextTunnelPort();

            final Session session =
                SECURE_CHANNEL.getSession(TUNNEL_USER,TUNNEL_HOST,TUNNEL_PORT);
            session.setConfig(new Properties(){{
                put("StrictHostKeyChecking", "no");
            }});
            session.setPortForwardingL(tunnelPort, hostName, 3306);

            final Connection connection =
                DriverManager.getConnection(
                    String.format(CONNECTION_URL_FORMAT, tunnelPort),
                    mysqlConnectionProperties);

            final TunneledConnection tunneledConnection =
                new TunneledConnection(session, connection);

            TUNNELED_CONNECTION_MAP.put(hostName, tunneledConnection);
        }

        return TUNNELED_CONNECTION_MAP.get(hostName);
    }

    private static Integer getNextTunnelPort() {
        return lastTunnelPort++;
    }

    public static class TunneledConnection {
        private final Session session;
        private final Connection connection;

        public TunneledConnection(Session session, Connection connection) {
            this.session = session;
            this.connection = connection;
        }

        public Session getSession() {
            return session;
        }

        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (! (o instanceof TunneledConnection)) return false;

            TunneledConnection that = (TunneledConnection) o;

            if (! session.equals(that.session)) return false;
            return connection.equals(that.connection);
        }

        @Override
        public int hashCode() {
            int result = session.hashCode();
            result = 31 * result + connection.hashCode();
            return result;
        }
    }
}
