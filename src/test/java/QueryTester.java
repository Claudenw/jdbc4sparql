import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.J4SConnectionTest;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.J4SDriverTest;
import org.xenei.jdbc4sparql.J4SUrl;
import org.xenei.jdbc4sparql.LoggingConfig;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;

public class QueryTester
{

	
	public static void main( final String[] args ) throws Exception
	{
		String qry = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT  (sum(?x) as ?y) WHERE { [] <http://example.com/int> ?x1 . bind( xsd:integer(?x1) as ?x) }";
		Model m = ModelFactory.createDefaultModel();
		Property d = m.createProperty( "http://example.com/double");
		Property i = m.createProperty( "http://example.com/int");
		m.add( m.createResource("http://example.com/A"), d, m.createTypedLiteral(-1.3) );
		m.add( m.createResource("http://example.com/A"), i, m.createTypedLiteral(-3) );
		m.add( m.createResource("http://example.com/B"), d, m.createTypedLiteral(1.5) );
		m.add( m.createResource("http://example.com/B"), i, m.createTypedLiteral(5) );
		m.add( m.createResource("http://example.com/C"), d, m.createTypedLiteral(1.7) );
		m.add( m.createResource("http://example.com/C"), i, "7" );
		Query q = QueryFactory.create(qry);
		 QueryExecution qexec = QueryExecutionFactory.create(q,m);
		final List<QuerySolution> retval = WrappedIterator.create(
					qexec.execSelect()).toList();
		System.out.println(retval);	
	}

}
