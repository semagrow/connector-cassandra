package eu.semagrow.cassandra.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by antonis on 5/4/2016.
 */
public class CassandraClient {

    private final Logger logger = LoggerFactory.getLogger(CassandraClient.class);

    private String address;
    private int port;
    private String keyspace;

    private Cluster cluster;
    private Session session;

    private static CassandraClient instance = null;

    private CassandraClient() {}

    public static CassandraClient getInstance(String address, int port, String keyspace) {
        if (instance == null) {
            instance = new CassandraClient();
            instance.setCredentials(address, port, keyspace);
            instance.connect();
        }
        return instance;
    }

    private void setCredentials(String address, int port, String keyspace) {
        this.address = address;
        this.port = port;
        this.keyspace = keyspace;
    }

    private void connect() {
        cluster = Cluster.builder().addContactPoint(address).withPort(port).build();
        session = cluster.connect();
        session.execute("USE " + keyspace + ";");
    }

    private void close() {
        session.close();
        cluster.close();
    }

    public Iterable<Row> execute(String query) {
        logger.info("Sending query: {}", query);
        ResultSet results = session.execute(query);
        return results.all();
    }

}
