import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;


public class SparqlDisplay {

	public SparqlDisplay() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = QueryFactory.create("SELECT (MAX(?tstamp) as ?maxts) (MAX(?state) as ?x) WHERE {" +
				"	      ?state a <http://example/ConnectionState> ." +
				"	     ?state <http://example/timestamp> ?tstamp ." +
				"	    }" );
		q.toString();
	}

}
