package eu.semagrow.cassandra.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import eu.semagrow.cassandra.vocab.CDT;
import org.apache.commons.lang3.StringUtils;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

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

    public static IRI getSubjectURIFromRow(String base, String table, Row row, Set<String> publicKey) {
        Set<String> substrings = new HashSet<>();
        for (String column : publicKey) {
            substrings.add(column + "=" + getStringFromCassandraResult(row, column));
        }
        String uriString = null;
        try {
            uriString = base + "/" + table + "#" + URLEncoder.encode(StringUtils.join(substrings,";"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return vf.createIRI(uriString);
    }

    public static Value getLiteralFromCassandraResult(Row row, String columnname) {
        DataType dataType = row.getColumnDefinitions().getType(columnname);

        if (row.getObject(columnname) == null) {
            return vf.createLiteral("");
        }

        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text()) || dataType.equals(DataType.inet())) {
            String result = row.getString(columnname);
            if (result.startsWith("<") && result.endsWith(">")) {
                return vf.createIRI(result.substring(1,result.length()-1));
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
            return vf.createLiteral(row.getObject(columnname).toString(), CDT.BLOB);
        }
        if (dataType.equals(DataType.date())) {
            return vf.createLiteral(row.getDate(columnname).toString(), CDT.DATE);
        }
        if (dataType.equals(DataType.time())) {
            return vf.createLiteral(row.getObject(columnname).toString(), CDT.TIME);
        }
        if (dataType.equals(DataType.timestamp())) {
            return vf.createLiteral(row.getTimestamp(columnname).toString(), CDT.TIMESTAMP);
        }
        if (dataType.equals(DataType.timeuuid())) {
            return vf.createLiteral(row.getObject(columnname).toString(), CDT.TIMEUUID);
        }
        if (dataType.isFrozen()) {
            if (dataType.getName().toString().equals("udt")) {
                return vf.createLiteral(row.getUDTValue(columnname).toString(), CDT.UDT);
            }
        }
        if (dataType.isCollection()) {
            if (dataType.getName().toString().equals("set")) {
                return vf.createLiteral(row.getObject(columnname).toString(), CDT.SET);
            }
            if (dataType.getName().toString().equals("list")) {
                return vf.createLiteral(row.getObject(columnname).toString(), CDT.LIST);
            }
            if (dataType.getName().toString().equals("map")) {
                return vf.createLiteral(row.getObject(columnname).toString(), CDT.MAP);
            }
        }
        throw new RuntimeException();
    }


    private static String getStringFromCassandraResult(Row row, String columnname) {
        DataType dataType = row.getColumnDefinitions().getType(columnname);

        if (row.getObject(columnname) == null) {
            return "";
        }

        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text()) || dataType.equals(DataType.inet())) {
            return "'" + row.getString(columnname) + "'";
        }
        if (dataType.equals(DataType.varint())) {
            return "" + row.getVarint(columnname).intValueExact();
        }
        if (dataType.equals(DataType.decimal())) {
            return "" + row.getDecimal(columnname).doubleValue();
        }
        if (dataType.equals(DataType.cint())) {
            return "" + row.getInt(columnname);
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return "" + row.getLong(columnname);
        }
        if (dataType.equals(DataType.cdouble())) {
            return "" + row.getDouble(columnname);
        }
        if (dataType.equals(DataType.cfloat())) {
            return "" + row.getFloat(columnname);
        }
        if (dataType.equals(DataType.cboolean())) {
            return "" + row.getBool(columnname);
        }
        else return "'" + row.getObject(columnname).toString() + "'";
    }



}
