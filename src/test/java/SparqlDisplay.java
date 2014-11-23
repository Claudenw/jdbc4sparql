import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class SparqlDisplay {

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final Query q = QueryFactory
				.create("SELECT (MAX(?tstamp) as ?maxts) (MAX(?state) as ?x) WHERE {"
						+ "	      ?state a <http://example/ConnectionState> ."
						+ "	     ?state <http://example/timestamp> ?tstamp ."
						+ "	    }");
		q.toString();
	}

	public SparqlDisplay() {
		// TODO Auto-generated constructor stub
	}

}
