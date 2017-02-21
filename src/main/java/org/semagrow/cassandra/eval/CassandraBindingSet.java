package org.semagrow.cassandra.eval;

import com.datastax.driver.core.Row;
import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.mapping.RdfMapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.SimpleBinding;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by angel on 21/4/2016.
 */
public class CassandraBindingSet implements BindingSet {

    private Row internalRep;
    private Map<String, String> var2column;
    private String subjectVar;
    private CassandraSchema cassandraSchema;
    private String base;
    private String table;

    private Resource subjectResource;

    public CassandraBindingSet(String subjectVar, String base, String table, CassandraSchema cassandraSchema, Row row, Map<String, String> var2column, ValueFactory vf) {
        internalRep = row;

        this.var2column = var2column;
        this.subjectVar = subjectVar;
        this.subjectResource = vf.createBNode(row.getColumnDefinitions().getTable(0));
        this.cassandraSchema = cassandraSchema;
        this.base = base;
        this.table = table;
    }

    @Override
    public Iterator<Binding> iterator() {
        return getBindingNames().stream()
                .map(c -> getBinding(c))
                .iterator();
    }

    @Override
    public Set<String> getBindingNames() {
        return Stream.concat(
                    Stream.of(subjectVar),
                    var2column.keySet().stream()
                       .filter(e -> hasBinding(e))).collect(Collectors.toSet());
    }

    @Override
    public Binding getBinding(String v) {

        if (v.equals(subjectVar)) {
            //return new BindingImpl(subjectVar, subjectResource);
            IRI uri = RdfMapper.getSubjectURIFromRow(base, table, internalRep, cassandraSchema.getPublicKey(table));
            return new SimpleBinding(subjectVar, uri);
        }
        else {
            String c = var2column.get(v);
            if (c != null) {
                Value value = RdfMapper.getLiteralFromCassandraResult(internalRep, c);
                return new SimpleBinding(v, value);
            }
            else {
                return null;
            }
        }
    }

    @Override
    public boolean hasBinding(String v) {

        if (v.equals(subjectVar))
            return true;

        String c = var2column.get(v);

        if (c == null)
            return false;

        return internalRep.getColumnDefinitions().getIndexOf(c) != -1;
    }

    @Override
    public Value getValue(String s) {
        if (getBinding(s) != null) {
            return getBinding(s).getValue();
        }
        else return null;
    }

    @Override
    public int size() {
        return internalRep.getColumnDefinitions().size() + 1;
    }

}
