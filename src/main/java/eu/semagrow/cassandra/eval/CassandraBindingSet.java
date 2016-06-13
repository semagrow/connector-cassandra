package eu.semagrow.cassandra.eval;

import com.datastax.driver.core.Row;
import eu.semagrow.cassandra.mapping.RdfMapper;
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

    private Resource subjectResource;

    public CassandraBindingSet(String subjectVar, Row row, Map<String, String> var2column, ValueFactory vf) {
        internalRep = row;

        this.var2column = var2column;
        this.subjectVar = subjectVar;
        this.subjectResource = vf.createBNode(row.getColumnDefinitions().getTable(0));
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

        if (v.equals(subjectVar))
            return new SimpleBinding(subjectVar, subjectResource);
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
        return getBinding(s).getValue();
    }

    @Override
    public int size() {
        return internalRep.getColumnDefinitions().size() + 1;
    }

}
