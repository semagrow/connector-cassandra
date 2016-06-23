package org.semagrow.cassandra.config;

import org.semagrow.cassandra.eval.CassandraQueryExecutorImpl;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.evaluation.QueryExecutorConfigException;
import org.semagrow.evaluation.QueryExecutorFactory;
import org.semagrow.evaluation.QueryExecutorImplConfig;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraQueryExecutorFactory implements QueryExecutorFactory {

    @Override
    public String getType() {
        return CassandraQueryExecutorConfig.TYPE;
    }

    @Override
    public QueryExecutorImplConfig getConfig() {
        return new CassandraQueryExecutorConfig();
    }

    @Override
    public QueryExecutor getQueryExecutor(QueryExecutorImplConfig config) throws QueryExecutorConfigException {
        return new CassandraQueryExecutorImpl();
    }

}
