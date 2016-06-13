package eu.semagrow.cassandra.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Created by antonis on 8/4/2016.
 */
public class RdfMapper {

    private static final ValueFactory vf = SimpleValueFactory.getInstance();

    public static IRI getUriFromTable(String base, String table) {
        String uriString = base + "/" + table;
        return vf.createIRI(uriString);
    }

    public static IRI getUriFromColumn(String base, String table, String column) {
        String uriString = base + "/" + table + "#" + column;
        return vf.createIRI(uriString);
    }

    public static IRI getXsdFromColumnDatatype(DataType dataType) {
        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#string");
        }
        if (dataType.equals(DataType.varint())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#integer");
        }
        if (dataType.equals(DataType.decimal())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#decimal");
        }
        if (dataType.equals(DataType.cint())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#int");
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#long");
        }
        if (dataType.equals(DataType.cdouble())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#double");
        }
        if (dataType.equals(DataType.cfloat())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#float");
        }
        if (dataType.equals(DataType.cboolean())) {
            return vf.createIRI("http://www.w3.org/2001/XMLSchema#boolean");
        }
        throw new RuntimeException();  /*blob, inet, timestamp, uuid, timeuuid, list, map, set*/
    }

    public static Value getLiteralFromCassandraResult(Row row, String columnname) {
        DataType dataType = row.getColumnDefinitions().getType(columnname);

        if (row.getObject(columnname) == null) {
            return vf.createLiteral("");
        }

        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text())) {
            String result = row.getString(columnname);
            if (result.startsWith("<") && result.endsWith(">")) {
                return vf.createIRI(result.substring(1,result.length()-1));
            }
            else {
                return vf.createLiteral(result);
            }
        }
        if (dataType.equals(DataType.varint())) {
            return vf.createLiteral(row.getVarint(columnname).doubleValue());
        }
        if (dataType.equals(DataType.decimal())) {
            return vf.createLiteral(row.getDecimal(columnname).doubleValue());
        }
        if (dataType.equals(DataType.cint())) {
            return vf.createLiteral(row.getInt(columnname));
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return vf.createLiteral(row.getLong(columnname));
        }
        if (dataType.equals(DataType.cdouble())) {
            return vf.createLiteral(row.getDouble(columnname));
        }
        if (dataType.equals(DataType.cfloat())) {
            return vf.createLiteral(row.getFloat(columnname));
        }
        if (dataType.equals(DataType.cboolean())) {
            return vf.createLiteral(row.getBool(columnname));
        }
        throw new RuntimeException();  /*blob, inet, timestamp, uuid, timeuuid, list, map, set, isNULL */
    }


}
