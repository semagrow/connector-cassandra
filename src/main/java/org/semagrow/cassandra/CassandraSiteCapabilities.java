package org.semagrow.cassandra;

import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.mapping.CqlMapper;
import org.semagrow.cassandra.utils.Utils;
import org.semagrow.plan.Plan;
import org.semagrow.selector.AbstractSiteCapabilities;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by angel on 31/3/2016.
 */
public class CassandraSiteCapabilities extends AbstractSiteCapabilities {

    private CassandraSchema cassandraSchema;
    private String base;
    ValueFactory vf = SimpleValueFactory.getInstance();

    public CassandraSiteCapabilities(CassandraSchema cassandraSchema, String base) {
        this.cassandraSchema = cassandraSchema;
        this.base = base;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean canExecute(Plan p) {
        return acceptsBindings(p, new HashSet<>());
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean isJoinable(Plan p1, Plan p2) {

        /* both plans must contain triple patterns with the same subject
         * all triple pattern data must be contained in the same table
         * and all object variables must be different. */

        final Set<Var> subjects = new HashSet<>();
        final Set<Var> predicates = new HashSet<>();
        final List<String> objectVars = new ArrayList<>();

        p1.visit(new AbstractQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(StatementPattern node) throws RuntimeException {
                subjects.add(node.getSubjectVar());
                predicates.add(node.getPredicateVar());
                if (!node.getObjectVar().hasValue()) {
                    objectVars.add(node.getObjectVar().getName());
                }
            }
        });

        p2.visit(new AbstractQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(StatementPattern node) throws RuntimeException {
                subjects.add(node.getSubjectVar());
                predicates.add(node.getPredicateVar());
                if (!node.getObjectVar().hasValue()) {
                    objectVars.add(node.getObjectVar().getName());
                }
            }
        });
        return (subjects.size() == 1 && containedInSameTable(predicates) &&
                objectVars.stream().count() == objectVars.stream().distinct().count());
    }

    /* checks if all the columns that correspond to each predicate are contained in the same cassandra table */

    private boolean containedInSameTable(Collection<Var> predicates) {
        return (predicates.stream()
                .noneMatch(p -> p.getValue() == null) &&
                predicates.stream()
                        .map(p -> CqlMapper.getTableFromURI(base, (IRI) p.getValue(), cassandraSchema))
                        .distinct().count() == 1);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean acceptsBindings(Plan plan, Set<String> vars) {

        /* get table and restricted columns */
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(plan);
        try {
            String table = getRelevantTable(statementPatterns);
            Set<String> restrictedColumns = getRestrictedColumns(statementPatterns, vars);
            /* checks if the restricted columns in the plan can be actually restricted in cassandra */
            return cassandraSchema.canRestrictColumns(restrictedColumns, table);
        } catch (IllegalStateException e) {
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean acceptsFilter(Plan plan, ValueExpr cond) {

        /* get table and restricted columns */

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(plan);
        String table = getRelevantTable(statementPatterns);

        if (cond instanceof Compare) {
            Compare compare = (Compare) cond;
            if ((compare.getOperator().equals(Compare.CompareOp.EQ)) ||
                    (compare.getOperator().equals(Compare.CompareOp.GE)) ||
                    (compare.getOperator().equals(Compare.CompareOp.GT)) ||
                    (compare.getOperator().equals(Compare.CompareOp.LE)) ||
                    (compare.getOperator().equals(Compare.CompareOp.LT))) {

                Var var;

                if (compare.getLeftArg() instanceof Var) {
                    var = ((Var) compare.getLeftArg());
                } else {
                    var = ((Var) compare.getRightArg());
                }

                Set<String> set = new HashSet<>();
                set.add(var.getName());

                Set<String> restrictedColumns = getRestrictedColumns(statementPatterns, set);
                return cassandraSchema.canRestrictColumns(restrictedColumns, table);
            }
            else {
                return false;
            }
        }
        else {
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public TupleExpr enforceSite(Plan p) {

        Plan plan = (Plan) p.clone();

        /* get table and restricted columns */

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(plan);
        if ((statementPatterns.size() == 1) && (!statementPatterns.get(0).getPredicateVar().hasValue())) {
            Plan newPlan = new Plan(statementPatterns.get(0));
            newPlan.setProperties(plan.getProperties());
            return newPlan;
        }

        String table = getRelevantTable(statementPatterns);
        if (table == null) {
            return new EmptySet();
        }
        Set<String> restrictedColumns = getRestrictedColumns(statementPatterns, new HashSet<>());

        /* get all columns that are restricted and cannot be restrictable */

        Set<String> nonRestrictableColumns = cassandraSchema.getNonRestrictableColumns(restrictedColumns, table);

        List<ValueExpr> restrictions = new ArrayList<>();

        /* for all non restrictable columns create a filter condition */

        plan.visit(new AbstractQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(StatementPattern node) throws RuntimeException {

                String column = CqlMapper.getColumnFromURI(base, (IRI) node.getPredicateVar().getValue(), cassandraSchema);
                Value value = node.getObjectVar().getValue();

                if (nonRestrictableColumns.contains(column) && value != null) {
                    Var newvar = new Var(UUID.randomUUID().toString());
                    Compare restriction = new Compare(newvar, new ValueConstant(value), Compare.CompareOp.EQ);
                    node.setObjectVar(newvar);
                    restrictions.add(restriction);
                }
            }
        });

        if (restrictions.size() == 0) {
            return p;
        }

        ValueExpr condition = restrictions.stream()
                .reduce(new ValueConstant(vf.createLiteral(true)),
                        (cmp1, cmp2) -> new And(cmp1, cmp2));

        //return new Plan(p.getKey(),
        //        new Filter(new SourceQuery(plan,site), condition));

        return new Filter(plan, condition);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /* helpers */

    private String getRelevantTable(List<StatementPattern> statementPatterns) {
        return statementPatterns.stream()
                .map(pattern -> ((IRI) pattern.getPredicateVar().getValue()))
                .filter(uri -> CqlMapper.getTableFromURI(base, (IRI) uri, cassandraSchema) != null)
                .map(uri -> CqlMapper.getTableFromURI(base, (IRI) uri, cassandraSchema))
                .distinct()
                .collect(Utils.singletonCollector());
    }

    private Set<String> getRestrictedColumns(List<StatementPattern> statementPatterns, Set<String> boundVars) {
         return statementPatterns.stream()
                 .filter(pattern -> isBound(pattern, boundVars))
                 .map(pattern -> ((IRI) pattern.getPredicateVar().getValue()))
                 .filter(uri -> CqlMapper.getColumnFromURI(base, (IRI) uri, cassandraSchema) != null)
                 .map(uri -> CqlMapper.getColumnFromURI(base, (IRI) uri, cassandraSchema))
                 .collect(Collectors.toSet());
    }

    /* a pattern is bound if its object if is not a variable or if the object variable is contined in boundVars set. */

    private boolean isBound(StatementPattern pattern, Set<String> boundVars) {
        return (pattern.getObjectVar().getValue() != null ||
                boundVars.stream().anyMatch(var -> var.equals(pattern.getObjectVar().getName())));
    }
}
