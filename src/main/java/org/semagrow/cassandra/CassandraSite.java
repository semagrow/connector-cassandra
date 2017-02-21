package org.semagrow.cassandra;

import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.connector.CassandraSchemaInit;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSite implements Site {

    static final String TYPE = "CASSANDRA";

    private final IRI endpoint;

    public CassandraSite(IRI endpoint) { this.endpoint = endpoint; }

    public Resource getID() { return getURI(); }

    public String getType() { return TYPE; }

    public IRI getURI() { return endpoint; }

    @Override
    public boolean isRemote() { return true; }

    @Override
    public SiteCapabilities getCapabilities() {
        return new CassandraSiteCapabilities(getCassandraSchema(), getBase());
    }

    public CassandraSchema getCassandraSchema() {
        return CassandraSchemaInit.getInstance().getCassandraSchema(endpoint);
    }

    public String getBase() {
        return getCassandraSchema().getBase();
    }

    public String getAddress() {
        return getCassandraSchema().getAddress();
    }

    public int getPort() {
        return getCassandraSchema().getPort();
    }

    public String getKeyspace() {
        return getCassandraSchema().getKeyspace();
    }

    public String toString() {
        return endpoint.toString();
    }

}
