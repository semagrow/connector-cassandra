package org.semagrow.cassandra.config;

import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorImplConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraQueryExecutorConfig implements QueryExecutorImplConfig {

    public static String TYPE = "CASSANDRA";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() throws QueryExecutorConfigException {

    }

    public Resource export(Model graph) {
        return null;
    }

    public void parse(Model graph, Resource resource) throws QueryExecutorConfigException {

    }

}
