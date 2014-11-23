import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.net.URL;
import java.util.List;
import org.xenei.jdbc4sparql.J4SConnectionTest;

public class QueryTester {

	public static void main(final String[] args) throws Exception {
		// String qry =
		// "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT  (sum(?x) as ?y) WHERE { [] <http://example.com/int> ?x1 . bind( xsd:integer(?x1) as ?x) }";
		Model m = ModelFactory.createDefaultModel();
		m.read(fUrl.openStream(), "http://example.com/", "TURTLE");
		// Property d = m.createProperty( "http://example.com/double");
		// Property i = m.createProperty( "http://example.com/int");
		// m.add( m.createResource("http://example.com/A"), d,
		// m.createTypedLiteral(-1.3) );
		// m.add( m.createResource("http://example.com/A"), i,
		// m.createTypedLiteral(-3) );
		// m.add( m.createResource("http://example.com/B"), d,
		// m.createTypedLiteral(1.5) );
		// m.add( m.createResource("http://example.com/B"), i,
		// m.createTypedLiteral(5) );
		// m.add( m.createResource("http://example.com/C"), d,
		// m.createTypedLiteral(1.7) );
		// m.add( m.createResource("http://example.com/C"), i, "7" );
		Query q = QueryFactory.create(qry);
		QueryExecution qexec = QueryExecutionFactory.create(q, m);
		final List<QuerySolution> retval = WrappedIterator.create(
				qexec.execSelect()).toList();
		for (QuerySolution qs : retval) {
			System.out.println(qs);
		}
	}

	static URL fUrl = J4SConnectionTest.class
			.getResource("./J4SStatementTest.ttl");

	static String qry = "SELECT  * "
			+ "			WHERE"
			+ "			  { { { { ?fooTable <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/jdbc4sparql#fooTable> ."
			+ "			          ?fooTable <http://example.com/jdbc4sparql#IntCol> ?v_b3f2fd82_c102_3c4d_baed_5958c464a424 ."
			+ "			          ?fooTable <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v_599dcce2_998a_3b40_b1e3_8e8c6006cb0a ."
			+ "			          ?fooTable <http://example.com/jdbc4sparql#StringCol> ?v_f20fd591_2dfa_3a09_b81b_7c6f31cc3159"
			+ "			        }"
			+ "			        OPTIONAL"
			+ "			          { ?fooTable <http://example.com/jdbc4sparql#NullableStringCol> ?v_2ca911d9_9e97_3d80_aaae_d6347f341e4e}"
			+ "			        OPTIONAL"
			+ "			          { ?fooTable <http://example.com/jdbc4sparql#NullableIntCol> ?v_ce84b044_b71d_37a4_bc63_462bd432993c}"
			+ "			      }"
			+ "			 BIND((?v_b3f2fd82_c102_3c4d_baed_5958c464a424) AS ?IntCol)"
			+ "		      BIND((?v_ce84b044_b71d_37a4_bc63_462bd432993c) AS ?NullableIntCol)"
			+ "		      BIND((?v_599dcce2_998a_3b40_b1e3_8e8c6006cb0a) AS ?type)"
			+ "		      BIND((?v_f20fd591_2dfa_3a09_b81b_7c6f31cc3159) AS ?StringCol)"
			+ "		      BIND((?v_2ca911d9_9e97_3d80_aaae_d6347f341e4e) AS ?NullableStringCol)"
			+ "			    }" + "			  }";
}
