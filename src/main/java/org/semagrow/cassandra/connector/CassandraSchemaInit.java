package org.semagrow.cassandra.connector;

import org.semagrow.cassandra.vocab.CDT;
import org.semagrow.cassandra.vocab.CDV;
import org.semagrow.util.FileUtils;
import org.semagrow.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParserRegistry;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by antonis on 12/4/2016.
 */
public class CassandraSchemaInit {

    private IRI[] complexDatatypes = { CDT.MAP, CDT.SET, CDT.LIST }; // add more if something doesn't work

    private static final String queryPrefix = "" +
            "PREFIX cdv:  <"  + CDV.NAMESPACE  + "> \n" +
            "PREFIX void: <" + VOID.NAMESPACE  + "> \n ";

    private Map<String, CassandraSchema> schemaMap = new HashMap<>();

    private CassandraSchemaInit() {}

    private static CassandraSchemaInit instance = null;

    public static CassandraSchemaInit getInstance() {
        if (instance == null) {
            File file = null;
            try {
                file = FileUtils.getFile("metadata.ttl");
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance = new CassandraSchemaInit();
            instance.initialize(file);
        }
        return instance;
    }

    public CassandraSchema getCassandraSchema(IRI endpoint) {
        return schemaMap.get(endpoint.stringValue());
    }

    private void initialize(File file) {

        try {
            Repository repo = new SailRepository(new MemoryStore());
            repo.initialize();

            RDFFormat fileFormat = RDFFormat.matchFileName(file.getAbsolutePath(), RDFParserRegistry.getInstance().getKeys()).orElse(RDFFormat.N3);

            RepositoryConnection conn = repo.getConnection();
            conn.add(file, file.toURI().toString(), fileFormat);

            initializeMap(conn);
            processCredentials(conn);
            processTables(conn);
            processColumns(conn);
            processIndices(conn);

            conn.close();
            repo.shutDown();

        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initializeMap(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = new CassandraSchema();
                schemaMap.put(bs.getValue("endpoint").stringValue(), cs);
            }
        } finally {
            results.close();
        }
    }

    private void processCredentials(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?address ?port ?keyspace ?base WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?d cdv:address ?address . \n" +
                "   ?d cdv:port ?port . \n" +
                "   ?d cdv:keyspace ?keyspace . \n" +
                "   ?d cdv:base ?base . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                cs.setBase(bs.getValue("base").stringValue());
                cs.setCredentials(
                        bs.getValue("address").stringValue(),
                        Integer.valueOf(bs.getValue("port").stringValue()),
                        bs.getValue("keyspace").stringValue()
                );
            }
        } finally {
            results.close();
        }
    }

    private void processTables(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?name WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?d cdv:base ?base . \n" +
                "   ?d cdv:tables ?table . \n" +
                "   ?table cdv:name ?name . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                cs.addTable(bs.getValue("name").stringValue());
            }
        } finally {
            results.close();
        }
    }

    private void processColumns(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?columnname ?tablename ?type ?datatype ?position WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?table cdv:name ?tablename . \n" +
                "   ?table cdv:tableSchema ?schema . \n" +
                "   ?schema cdv:columns ?column . \n" +
                "   ?column cdv:columnType ?type . \n" +
                "   ?column cdv:name ?columnname . \n" +
                "   ?column cdv:datatype ?datatype . \n" +
                "   OPTIONAL { ?column cdv:clusteringPosition ?position . } \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                IRI type = (IRI) bs.getValue("type");
                String column = bs.getValue("columnname").stringValue();
                String table = bs.getValue("tablename").stringValue();
                IRI datatype = (IRI) bs.getValue("datatype");

                if (type.equals(CDV.PARTITION)) {
                    cs.addPartitionColumn(table, column);
                }
                if (type.equals(CDV.CLUSTERING)) {
                    int position = Integer.valueOf(bs.getValue("position").stringValue());
                    cs.addClusteringColumn(table, column, position);
                }
                if (type.equals(CDV.REGULAR)) {
                    cs.addRegularColumn(table, column);
                }
                for (IRI uri: complexDatatypes) {
                    if (uri.equals(datatype)) {
                        cs.makeComplex(table,column);
                    }
                }
            }
        } finally {
            results.close();
        }
    }

    private void processIndices(RepositoryConnection conn)
            throws MalformedQueryException, QueryEvaluationException, RepositoryException {

        String query = queryPrefix +
                "SELECT ?endpoint ?columnname ?tablename WHERE { \n" +
                "   ?d rdf:type cdv:cassandraDB . \n" +
                "   ?d void:sparqlEndpoint ?endpoint . \n" +
                "   ?table cdv:name ?tablename . \n" +
                "   ?table cdv:tableSchema ?schema . \n" +
                "   ?schema cdv:secondaryIndex ?columnname . \n" +
                "}";

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        TupleQueryResult results = tupleQuery.evaluate();
        try {
            while (results.hasNext()) {
                BindingSet bs = results.next();
                CassandraSchema cs = schemaMap.get(bs.getValue("endpoint").stringValue());

                String column = bs.getValue("columnname").stringValue();
                String table = bs.getValue("tablename").stringValue();

                cs.addIndex(table,column);
            }
        } finally {
            results.close();
        }
    }
}
