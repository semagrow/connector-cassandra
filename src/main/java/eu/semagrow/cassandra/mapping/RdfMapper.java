package eu.semagrow.cassandra.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import eu.semagrow.cassandra.vocab.CDT;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 8/4/2016.
 */
public class RdfMapper {

    private static final ValueFactory vf = ValueFactoryImpl.getInstance();

    public static URI getUriFromTable(String base, String table) {
        String uriString = base + "/" + table;
        return vf.createURI(uriString);
    }

    public static URI getUriFromColumn(String base, String table, String column) {
        String uriString = base + "/" + table + "#" + column;
        return vf.createURI(uriString);
    }

    public static Value getLiteralFromCassandraResult(Row row, String columnname) {
        DataType dataType = row.getColumnDefinitions().getType(columnname);

        if (row.getObject(columnname) == null) {
            return vf.createLiteral("");
        }

        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text()) || dataType.equals(DataType.inet())) {
            String result = row.getString(columnname);
            if (result.startsWith("<") && result.endsWith(">")) {
                return vf.createURI(result.substring(1,result.length()-1));
            }
            else {
                return vf.createLiteral(result);
            }
        }
        if (dataType.equals(DataType.varint())) {
            return vf.createLiteral(row.getVarint(columnname).intValueExact());
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
        if (dataType.equals(DataType.blob())) {
            return vf.createLiteral(row.getString(columnname), CDT.BLOB);
        }
        if (dataType.equals(DataType.date())) {
            return vf.createLiteral(row.getString(columnname), CDT.DATE);
        }
        if (dataType.equals(DataType.time())) {
            return vf.createLiteral(row.getString(columnname), CDT.TIME);
        }
        if (dataType.equals(DataType.timestamp())) {
            return vf.createLiteral(row.getString(columnname), CDT.TIMESTAMP);
        }
        if (dataType.equals(DataType.timeuuid())) {
            return vf.createLiteral(row.getString(columnname), CDT.TIMEUUID);
        }
        if (dataType.isFrozen()) {
            if (dataType.getName().toString().equals("udt")) {
                return vf.createLiteral(row.getString(columnname), CDT.UDT);
            }
        }
        if (dataType.isCollection()) {
            if (dataType.getName().toString().equals("set")) {
                return vf.createLiteral(row.getString(columnname), CDT.SET);
            }
            if (dataType.getName().toString().equals("list")) {
                return vf.createLiteral(row.getString(columnname), CDT.LIST);
            }
            if (dataType.getName().toString().equals("map")) {
                return vf.createLiteral(row.getString(columnname), CDT.MAP);
            }
        }
        throw new RuntimeException();
    }


}
