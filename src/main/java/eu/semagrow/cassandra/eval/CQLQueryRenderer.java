package eu.semagrow.cassandra.eval;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.queryrender.QueryRenderer;

import java.util.Collections;

/**
 * Created by angel on 21/4/2016.
 */
public class CQLQueryRenderer implements QueryRenderer {

    private static QueryLanguage CQL = new QueryLanguage("CQL");

    public QueryLanguage getLanguage() {
        return CQL;
    }

    public String render(ParsedQuery parsedQuery) throws Exception {
        CassandraQueryTransformer transformer = new CassandraQueryTransformer();
        return transformer.transformQuery(null, parsedQuery.getTupleExpr(), Collections.emptyList());
    }
}
