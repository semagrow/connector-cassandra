package org.semagrow.cassandra.config;

import org.semagrow.selector.SiteConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSiteConfig implements SiteConfig {

    public static String TYPE = "CASSANDRA";

    private IRI endpoint;
    private String id;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() { }

    @Override
    public String getSiteId() {
        return id;
    }

    @Override
    public void setSiteId(String s) {
        id = s;
    }

    public Resource export(Model graph) {
        return null;
    }

    public void parse(Model graph, Resource resource) { }

    public IRI getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(IRI endpoint) {
        this.endpoint = endpoint;
    }
}
