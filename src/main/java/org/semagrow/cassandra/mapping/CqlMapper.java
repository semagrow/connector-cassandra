package org.semagrow.cassandra.mapping;

import org.semagrow.cassandra.connector.CassandraSchema;
import org.semagrow.cassandra.vocab.CDT;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by antonis on 8/4/2016.
 */
public class CqlMapper {

    /**
     * Returns a table name from a URI.
     * Extracts the local name of the URI, and verifies it by looking the CassandraSchema
     * */
    public static String getTableFromURI(String base, IRI predicate, CassandraSchema schema) {
        Pair<String, String> pair = decompose(base, predicate);
        if (schema.tableContainsColumn(pair.getLeft(), pair.getRight())) {
            return pair.getLeft();
        }
        return null;
    }

    /**
     * Returns a column name from a URI.
     * Extracts the local name of the URI, and verifies it by looking the CassandraSchema
     * */
    public static String getColumnFromURI(String base, IRI predicate, CassandraSchema schema) {
        Pair<String, String> pair = decompose(base, predicate);
        if (schema.tableContainsColumn(pair.getLeft(), pair.getRight())) {
            return pair.getRight();
        }
        return null;
    }

    /**
     * Returns a table name from a URI.
     * Essentially extracts the local name of the URI
     * */
    public static String getTableFromURI(String base, IRI predicate) {
        Pair<String, String> pair = decompose(base, predicate);
        return pair.getLeft();
    }

    /**
     * Returns a column name from a URI.
     * Essentially extracts the local name of the URI
     * */
    public static String getColumnFromURI(String base, IRI predicate) {
        Pair<String, String> pair = decompose(base, predicate);
        return pair.getRight();
    }

    /**
     * Returns a cql substring that contains all relevant column restrictions
     */
    public static String getRestrictionsFromSubjectURI(String base, String table, IRI subject) {
        Pair<String, String> pair = decompose(base, subject);
        if (pair.getLeft().equals(table)) {
            try {
                return URLDecoder.decode(pair.getRight(),"UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException();
    }


    /***
     * Translates a Value to a CQLValue string representation
     * @param base
     * @param v
     * @return
     */
    public static String getCqlValueFromValue(String base, Value v) {
        if (v instanceof IRI) {
            if (v.stringValue().startsWith(base)) {
                return v.stringValue().substring(base.length());
            }
            else {
                return "\'<" + v.stringValue() + ">\'"; // maybe we dont need this
            }
        }
        if (v instanceof Literal) {
            if (isNumeric(((Literal) v))) {
                return v.stringValue();
            }
            else {
                if (((Literal) v).getDatatype() == null) {
                    return "\'" + v.stringValue() + "\'";
                }
                if (((Literal) v).getDatatype().equals(CDT.UDT)) {
                    return v.stringValue();
                }
                if (((Literal) v).getDatatype().equals(CDT.SET)) {
                    return v.stringValue();
                }
                return "\'" + v.stringValue() + "\'";
            }

        }
        else {
            return v.stringValue();
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static Pair<String, String> decompose(String base, IRI predicate) {

        if (predicate == null) {
            return Pair.of(null,null);
        }
        
        String value = predicate.stringValue();
        int columnStart = value.lastIndexOf("#") + 1;
        int predicateStart = value.lastIndexOf("/") + 1;

        if (predicateStart == -1) {
            //throw new RuntimeException();
            return Pair.of(null,null);
        }

        if (!(value.substring(0, predicateStart - 1).equals(base))) {
            //throw new RuntimeException("Predicate value is not contained in this Cassandra source.");
            return Pair.of(null,null);
        }

        String table = value.substring(predicateStart, columnStart - 1);

        if (columnStart == -1) {
            return Pair.of(table, null);
        }
        else {
            String column = value.substring(columnStart);
            return Pair.of(table, column);
        }
    }

    private static boolean isNumeric(Literal l) {

        IRI dataType =l.getDatatype();

        return  dataType != null && (dataType.equals(XMLSchema.INTEGER)
                || dataType.equals(XMLSchema.INT)
                || dataType.equals(XMLSchema.UNSIGNED_INT)
                || dataType.equals(XMLSchema.LONG)
                || dataType.equals(XMLSchema.UNSIGNED_LONG)
                || dataType.equals(XMLSchema.DOUBLE)
                || dataType.equals(XMLSchema.FLOAT));

    }
}
