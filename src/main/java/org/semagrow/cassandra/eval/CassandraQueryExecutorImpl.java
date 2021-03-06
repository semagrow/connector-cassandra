package org.semagrow.cassandra.eval;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.reactivestreams.Publisher;
import org.semagrow.cassandra.CassandraSite;
import org.semagrow.cassandra.connector.CassandraClient;
import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.connector.CassandraSchemaInit;
import org.semagrow.cassandra.mapping.CqlMapper;
import org.semagrow.cassandra.mapping.RdfMapper;
import org.semagrow.cassandra.utils.BindingSetOpsImpl;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.selector.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;


/**
 * Created by antonis on 21/3/2016.
 */
public class CassandraQueryExecutorImpl implements QueryExecutor {

    /// TODO: CHECK exceptions

    private final Logger logger = LoggerFactory.getLogger(CassandraQueryExecutorImpl.class);

    protected BindingSetOps bindingSetOps = new BindingSetOpsImpl();


    @Override
    public Publisher<BindingSet> evaluate(Site site, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return evaluateCassandraImpl((CassandraSite) site, expr, bindings);
    }

    @Override
    public Publisher<BindingSet> evaluate(Site site, TupleExpr expr, List<BindingSet> bindingList)
            throws QueryEvaluationException {
        try {
            return evaluateCassandraImpl((CassandraSite) site, expr, bindingList);
        }catch(QueryEvaluationException e)
        {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr) {
        return sendCqlQuery(site, expr, Collections.emptyList());
    }

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        if (bindings.size() == 0) {
            return evaluateCassandraImpl(site, expr);
        }

        return evaluateCassandraImpl(site, expr, Collections.singletonList(bindings));
    }

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException {

        if (bindings.isEmpty()) {
            return evaluateCassandraImpl(site, expr);
        }
        else {
            Set<String> freeVars = computeVars(expr);
            freeVars.removeAll(bindings.get(0).getBindingNames());
            Set<String> commonVars = computeVars(expr);
            commonVars.removeAll(freeVars);

            if (freeVars.isEmpty()) {
                return Streams.from(bindings).filter(b -> askCqlQuery(site, expr,b));
            }
            else {
                Map<BindingSet,List<BindingSet>> leftMultimap = createMultiMap(bindings, commonVars);
                Stream<BindingSet> result = sendCqlQuery(site, expr, bindings).flatMap(b -> lookup(leftMultimap, commonVars, b));
                return result;
            }
        }
    }

    private Stream<BindingSet> lookup(Map<BindingSet,List<BindingSet>> leftMultimap, Set<String> commonVars, BindingSet b) {
        BindingSet key = bindingSetOps.project(commonVars, b);
        List<BindingSet> leftList = leftMultimap.get(key);
        if (leftList == null) {
            return Streams.empty();
        }
        else {
            if (leftList.isEmpty()) {
                return Streams.empty();
            }
            else {
                return Streams.from(leftList).map(b1 -> bindingSetOps.merge(b, b1));
            }
        }
    }

    private Map<BindingSet,List<BindingSet>> createMultiMap(List<BindingSet> bindings, Set<String> commonVars) {
        Map<BindingSet,List<BindingSet>> multiMap = new HashMap<>();
        for (BindingSet b : bindings) {
            BindingSet key = bindingSetOps.project(commonVars, b);
            List<BindingSet> list = multiMap.get(key);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(b);
            multiMap.put(key,list);
        }
        return multiMap;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Stream<BindingSet> sendCqlQuery(CassandraSite site, TupleExpr expr, List<BindingSet> bindingsList) {

        if (checkPredVar(expr)) {
            return sendCqlQueryPredVar(site, expr, bindingsList);
        }

        CassandraQueryTransformer transformer = new CassandraQueryTransformer();

        String cqlQuery;
        Stream<BindingSet> result;

        try {
        	if (bindingsList.isEmpty()) {
        		cqlQuery = transformer.transformQuery(site.getBase(), site.getURI(), expr);
        	}
        	else {
        		cqlQuery = transformer.transformQuery(site.getBase(), site.getURI(), expr, bindingsList);
        	}
            CassandraClient client = CassandraClient.getInstance(site.getAddress(), site.getPort(), site.getKeyspace());

            logger.info("Sending CQL query: {} to {}:{}", cqlQuery, site.getAddress(), site.getPort());

            result = Streams.from(client.execute(cqlQuery))
                    .filter(transformer::containsAllFields)
                    .map(transformer::getBindingSet);

        }
        catch( NullPointerException ex ) {
        	// If the query cannot be expressed in CQL, then transformer::transformQuery() throws
        	// NullPointerException. We log this, but proceed to respond as if the query simply did not
        	// retrieve any results.
            logger.warn("Could not transform query: {}", expr);
        	result = Streams.empty();
        }
        catch( Exception ex ) {
            // Unexpected behaviour. Answering with empty responce but it needs invastigating
            logger.warn("Unexpected behaviour");
            result = Streams.empty();
        }

        return result;
    }

    private boolean askCqlQuery(CassandraSite site, TupleExpr expr, BindingSet bindings) {
        List<BindingSet> bindingsList = new ArrayList<>();
        bindingsList.add(bindings);
        Stream<BindingSet> result = sendCqlQuery(site, expr, bindingsList);
        return (!result.equals(Streams.empty()));
    }

    private Stream<BindingSet> sendCqlQueryPredVar(CassandraSite site, TupleExpr expr, List<BindingSet> bindingsList) {

        StatementPattern pattern = StatementPatternCollector.process(expr).get(0);
        CassandraSchema cassandraSchema = CassandraSchemaInit.getInstance().getCassandraSchema(site.getURI());
        CassandraClient client = CassandraClient.getInstance(site.getAddress(), site.getPort(), site.getKeyspace());
        String base = cassandraSchema.getBase();
        Stream<BindingSet> output;

        if (pattern.getSubjectVar().hasValue()) {
            IRI subject = (IRI) pattern.getSubjectVar().getValue();
            String table = CqlMapper.getTableFromURI(base, subject);
            String restrictions = CqlMapper.getRestrictionsFromSubjectURI(base, table, subject);
            String cqlQuery = "select * from " + table + " where " + restrictions.replace(";"," and ") + ";";

            output = Streams.from(client.execute(cqlQuery))
                    .flatMap(row -> Streams.from(row.getColumnDefinitions().asList())
                        .map(column -> {
                            IRI predValue = RdfMapper.getUriFromColumn(base, table, column.getName());
                            Value objValue = RdfMapper.getLiteralFromCassandraResult(row, column.getName());
                            QueryBindingSet bindings = new QueryBindingSet();
                            bindings.addBinding(pattern.getPredicateVar().getName(), predValue);
                            bindings.addBinding(pattern.getObjectVar().getName(), objValue);

                            return bindings;
                        })
                    );
        }
        else {
            output = Streams.from(cassandraSchema.getTables())
                    .map(table -> "select * from " + table +" allow filtering;")
                    .flatMap(cqlstring -> Streams.from(client.execute(cqlstring)))
                    .flatMap(row -> {
                        String table = row.getColumnDefinitions().getTable(0);
                        IRI subjValue = RdfMapper.getSubjectURIFromRow(base, table, row, cassandraSchema.getPublicKey(table));

                        return Streams.from(row.getColumnDefinitions().asList())
                            .map(column -> {
                                IRI predValue = RdfMapper.getUriFromColumn(base, table, column.getName());
                                Value objValue = RdfMapper.getLiteralFromCassandraResult(row, column.getName());
                                QueryBindingSet bindings = new QueryBindingSet();
                                bindings.addBinding(pattern.getSubjectVar().getName(), subjValue);
                                bindings.addBinding(pattern.getPredicateVar().getName(), predValue);
                                bindings.addBinding(pattern.getObjectVar().getName(), objValue);

                                return bindings;
                            });
                    });
        }
        return output.filter(bindings -> {
            if (pattern.getObjectVar().hasValue()) {
                return bindings.getValue(pattern.getObjectVar().getName()).equals(pattern.getObjectVar().getValue());
            }
            else return true;
        });
    }

    private boolean checkPredVar(TupleExpr expr) {
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);
        return ((statementPatterns.size() == 1) && (!statementPatterns.get(0).getPredicateVar().hasValue()));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Set<String> computeVars(TupleExpr serviceExpression) {
        final Set<String> res = new HashSet<String>();
        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException {
                // take only real vars, i.e. ignore blank nodes
                if (!node.hasValue() && !node.isAnonymous())
                    res.add(node.getName());
            }
            // TODO maybe stop tree traversal in nested SERVICE?
            // TODO special case handling for BIND
        });
        return res;
    }
}
