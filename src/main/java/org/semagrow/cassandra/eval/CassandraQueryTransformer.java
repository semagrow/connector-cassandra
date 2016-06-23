package org.semagrow.cassandra.eval;

import com.datastax.driver.core.Row;
import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.connector.CassandraSchemaInit;
import org.semagrow.cassandra.mapping.CqlMapper;
import org.semagrow.cassandra.utils.Utils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by antonis on 23/3/2016.
 */
public class CassandraQueryTransformer {

    private Var subject = null;
    private String table;
    private String base;

    private Map<String, String> var2column;
    private IRI endpoint;
    private CassandraSchema cassandraSchema;

    public String transformQuery(String base, IRI endpoint, TupleExpr expr) {
        return transformQuery(base, endpoint, expr, Collections.emptyList());
    }

    /**
     * CQL-fy a TupleExpr given a base, the actual expression and a list of bindings
     * @param base
     * @param expr
     * @param bindingSetList
     * @return
     */
    public String transformQuery(String base, IRI endpoint, TupleExpr expr, List<BindingSet> bindingSetList) {

        this.endpoint = endpoint;
        this.base = base;
        this.cassandraSchema = CassandraSchemaInit.getInstance().getCassandraSchema(endpoint);

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        /* get subject of all patterns: all patterns must have tha same subject */

        subject = statementPatterns.stream()
                .map(p -> p.getSubjectVar())
                .distinct()
                .collect(Utils.singletonCollector());

        /* get cassandra relevant table: all pattern data must be in the same table */

        table = statementPatterns.stream()
                .map(pattern -> (IRI) pattern.getPredicateVar().getValue())
                .map(uri -> CqlMapper.getTableFromURI(base, uri))
                .distinct()
                .collect(Utils.singletonCollector());

        /* get cassandra columns needed for CQL query renderer */

        Set<String> columns = statementPatterns.stream()
                .map(pattern -> (IRI) pattern.getPredicateVar().getValue())
                .map(uri -> CqlMapper.getColumnFromURI(base, uri))
                .collect(Collectors.toSet());
        columns.addAll(cassandraSchema.getPublicKey(table));


        /* get sparql variables => cassandra columns map: Map<sparqlVariable,CassandraTable>*/

        var2column = statementPatterns.stream()
                .filter(pattern -> pattern.getObjectVar().getValue() == null)
                .collect(Collectors.toMap(p -> p.getObjectVar().getName(),
                                          p -> CqlMapper.getColumnFromURI(base, (IRI) p.getPredicateVar().getValue())));

        /* get cassandra where restrictions */

        /* 1) restrictions from triple pattern objects */

        Stream<Restriction> patternRestrictions = statementPatterns.stream()
                .filter(p -> p.getObjectVar().getValue() != null)
                .filter(p -> (!((IRI) p.getPredicateVar().getValue()).equals(RDF.TYPE)))
                .map(p -> new Restriction(
                        CqlMapper.getColumnFromURI(base, (IRI) p.getPredicateVar().getValue()),
                        Compare.CompareOp.EQ,
                        CqlMapper.getCqlValueFromValue(base, p.getObjectVar().getValue())));

        /* 2) restrictions from bindingSet */

        Map<String, Set<String>> bindings = bindingSetList.stream()
                .flatMap(bindingSet -> StreamSupport.stream(bindingSet.spliterator(), false))
                .collect(Collectors.groupingBy(
                        binding -> binding.getName(),
                        Collectors.mapping(
                                binding -> CqlMapper.getCqlValueFromValue(base, binding.getValue()),
                                Collectors.toSet())));

        Stream<Restriction> bindingsRestrictions =
                bindings.entrySet().stream()
                        .filter( e -> var2column.containsKey(e.getKey()) )
                        .map( e -> new Restriction(var2column.get(e.getKey()), e.getValue()));


        /* 3) restrictions from triple pattern subjects and subject bindings */

        Set<Restriction> subjectRestrictions = new HashSet<>();
        Set<IRI> uriSet = new HashSet<>();

        if (subject.hasValue()) {
            uriSet.add((IRI) subject.getValue());
        }
        if (bindings.keySet().contains(subject.getName())) {
            for (BindingSet bs: bindingSetList) {
                uriSet.add((IRI) bs.getBinding(subject.getName()).getValue());
            }
        }
        addSubjectRestrictions(subjectRestrictions, base, table, uriSet);

        /* concat all restrictions */

        Set<Restriction> restrictions = Stream.concat(patternRestrictions, bindingsRestrictions).collect(Collectors.toSet());
        restrictions.addAll(subjectRestrictions);

        /* visit filters */

        expr.visit(new FilterRestrictionCollector(base, var2column, restrictions));

        return buildCqlQuery(table, columns, restrictions);
    }

    private String buildCqlQuery(String table, Set<String> columns, Set<Restriction> restrictions) {

        /* build restrictions of cql query */

        Set<String> wheres = restrictions.stream()
                .map(r -> r.getRestrictionString())
                .collect(Collectors.toSet());

        /* build query string */

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ");
        stringBuilder.append(StringUtils.join(columns, ", "));
        stringBuilder.append(" ");
        stringBuilder.append("FROM ");
        stringBuilder.append(table);
        stringBuilder.append(" ");
        if (!wheres.isEmpty()) {
            stringBuilder.append("WHERE ");
            stringBuilder.append(StringUtils.join(wheres, " AND "));
        }

        stringBuilder.append(" ALLOW FILTERING;");

        return stringBuilder.toString();
    }

    /**
     * Transforms a Cassandra Row to a BindingSet
     * @param row
     * @return
     */
    public BindingSet getBindingSet(Row row)
    {
        return new CassandraBindingSet(subject.getName(), base, table, cassandraSchema, row, var2column, SimpleValueFactory.getInstance());

        /*
        QueryBindingSet bindings = new QueryBindingSet();

        assert var2column != null && !var2column.keySet().isEmpty();
        assert subject != null;

        var2column.forEach((variable, columnname) -> {
            Value value = RdfMapper.getLiteralFromCassandraResult(row, columnname);
            Binding binding = new BindingImpl(variable, value);
            bindings.addBinding(binding);
        });

        Binding binding = new BindingImpl(subject, ValueFactoryImpl.getInstance().createBNode());
        bindings.addBinding(binding);
        return bindings;
        */

    }

    public boolean containsAllFields(Row row) {
        for (int i=0; i<row.getColumnDefinitions().size(); i++) {
            if (row.isNull(i)) {
                return false;
            }
        }
        return true;
    }

    private void addSubjectRestrictions(Set<Restriction> subjectRestrictions, String base, String table, Set<IRI> subjectBindings) {
        Map<String, Set<String>> columnRestrictions = new HashMap<>();
        for (IRI uri: subjectBindings) {
            String restrictionsString = CqlMapper.getRestrictionsFromSubjectURI(base, table, uri);
            for (String restrictionString : StringUtils.split(restrictionsString,";")) {
                String restriction[] = StringUtils.split(restrictionString, "=");
                String name = restriction[0];
                String value = restriction[1];
                Set<String> set = columnRestrictions.get(name);
                if (set == null) {
                    columnRestrictions.put(name, new HashSet<>());
                    set = columnRestrictions.get(name);
                }
                set.add(value);
            }
        }
        for (String name: columnRestrictions.keySet()) {
            subjectRestrictions.add(new Restriction(name, columnRestrictions.get(name)));
        }
    }

    private class Restriction {

        String column;
        Compare.CompareOp operator;
        String value;
        Set<String> values;

        public Restriction(String column, Compare.CompareOp operator, String value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
            this.values = null;
        }

        public Restriction(String column, Set<String> values) {
            this.column = column;
            this.operator = Compare.CompareOp.EQ;
            this.values = values;
        }

        String getRestrictionString() {
            if (value != null) {
                return column + operator.getSymbol() + value;
            } else {
                return column + " IN (" + StringUtils.join(values, ", ") + ")";
            }
        }

        public String toString() { return getRestrictionString(); }
    }

    private class FilterRestrictionCollector extends AbstractQueryModelVisitor<RuntimeException> {

        final private String base;
        private Map<String, String> var2column;
        final private Collection<Restriction> restrictions;

        public FilterRestrictionCollector(String base, Map<String, String> var2column, Collection<Restriction> restrictions) {
            this.base = base;
            this.var2column = var2column;
            this.restrictions = restrictions;
        }

        /*
        public static Collection<Restriction> process(TupleExpr expr) {
            final Collection<Restriction> coll = new LinkedList<>();
            expr.visit(new RestrictionCollector(coll));
            return coll;
        }
        */

        @Override
        public void meet(final Compare node) throws RuntimeException {

            // assertions
            assert (node.getLeftArg() instanceof Var && node.getRightArg() instanceof ValueConstant) ||
                    (node.getRightArg() instanceof Var && node.getLeftArg() instanceof ValueConstant);

            QueryModelNode x = node.getParentNode();
            while (!(x instanceof QueryRoot)) {
                if (!((x instanceof And) || (x instanceof Filter))) {
                    throw new RuntimeException();
                }
                if (x instanceof Filter) {
                    break;
                }
                x = x.getParentNode();
            }

            // find which is the variable and which is the value
            Var var;
            ValueConstant value;

            if (node.getLeftArg() instanceof Var) {
                var = ((Var) node.getLeftArg());
                value = ((ValueConstant) node.getRightArg());
            } else {
                var = ((Var) node.getRightArg());
                value = ((ValueConstant) node.getLeftArg());
            }

            String relevantColumn = var2column.get(var.getName());
            assert relevantColumn != null;

            Restriction restriction = new Restriction(relevantColumn, node.getOperator(), CqlMapper.getCqlValueFromValue(base, value.getValue()));

            restrictions.add(restriction);
        }
    }
}
