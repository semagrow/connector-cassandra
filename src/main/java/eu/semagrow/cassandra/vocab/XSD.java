package eu.semagrow.cassandra.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


/**
 * Created by antonis on 7/6/2016.
 */
public class XSD {

    public static final String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";

    public static final String PREFIX = "xsd";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public final static IRI STRING;
    public final static IRI INTEGER;
    public final static IRI DECIMAL;
    public final static IRI INT;
    public final static IRI LONG;
    public final static IRI DOUBLE;
    public final static IRI FLOAT;
    public final static IRI BOOLEAN;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        STRING = factory.createIRI(XSD.NAMESPACE, "string");
        INTEGER = factory.createIRI(XSD.NAMESPACE, "integer");
        DECIMAL = factory.createIRI(XSD.NAMESPACE, "decimal");
        INT = factory.createIRI(XSD.NAMESPACE, "int");
        LONG = factory.createIRI(XSD.NAMESPACE, "long");
        DOUBLE = factory.createIRI(XSD.NAMESPACE, "double");
        FLOAT = factory.createIRI(XSD.NAMESPACE, "float");
        BOOLEAN = factory.createIRI(XSD.NAMESPACE, "boolean");
    }
}
