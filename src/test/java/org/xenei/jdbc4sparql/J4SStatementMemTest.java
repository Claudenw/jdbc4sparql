package org.xenei.jdbc4sparql;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.Catalog;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

public class J4SStatementMemTest extends AbstractJ4SStatementTest {
	// file URL
	private URL fUrl;

	// J4SUrl
	private String url;

	@Before
	public void setup() throws Exception {
		LoggingConfig.setConsole(Level.DEBUG);
		LoggingConfig.setRootLogger(Level.INFO);
		LoggingConfig.setLogger("com.hp.hpl.jena.", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql", Level.INFO);
		LoggingConfig.setLogger("org.xenei.jdbc4sparql.sparql", Level.DEBUG );
		Class.forName("org.xenei.jdbc4sparql.J4SDriver");

		fUrl = J4SDriverTest.class.getResource("./J4SStatementTest.ttl");

		url = "jdbc:j4s?catalog=test&type=turtle&builder=org.xenei.jdbc4sparql.sparql.builders.SimpleNullableBuilder:"
				+ fUrl.toString();

		final Properties prop = new Properties();
		prop.setProperty(J4SPropertyNames.USER_PROPERTY, "myschema");
		prop.setProperty(J4SPropertyNames.PASSWORD_PROPERTY, "mypassw");
		conn = DriverManager.getConnection(url, prop);
		conn.setAutoCommit(false);
		stmt = conn.createStatement();
		// This is here to generate the zip file for reading config
//		 ((J4SConnection)conn).saveConfig( new
//		 java.io.File("/tmp/J4SStatementTest.zip"));
	}

	@After
	public void teardown() throws SQLException {
		stmt.close();
	}
	
	@Test
	@Ignore
	public void arbitraryQuery() throws Exception {
		final String queryStr =
"SELECT  ?StringCol ?IntCol ?type ?NullableStringCol ?NullableIntCol " + 
"WHERE" + 
"  { { { ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/jdbc4sparql#fooTable> . " + 
"        ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://example.com/jdbc4sparql#StringCol> ?v_88b57d74_b9f7_3ae1_84d5_303f1b18058b . " + 
"        ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://example.com/jdbc4sparql#IntCol> ?v_c46431c9_f473_33bc_a521_873ebb1d0854 . " + 
"        ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v_01b7f5c9_44ab_3338_a788_6c35c335963f" + 
"        OPTIONAL" + 
"          { ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://example.com/jdbc4sparql#NullableStringCol> ?v_caccfc47_d184_3d13_9990_16067ed22a39}" + 
"        OPTIONAL" + 
"          { ?v_a8445f5a_4be0_3251_ace0_6443e9719b01 <http://example.com/jdbc4sparql#NullableIntCol> ?v_270611a8_6b7e_3a2b_b560_4709cb515f5a}" + 
"        FILTER ( ( ( ( <java:org.xenei.jdbc4sparql.sparql.CheckTypeF>(?v_88b57d74_b9f7_3ae1_84d5_303f1b18058b, 12, false) && <java:org.xenei.jdbc4sparql.sparql.CheckTypeF>(?v_c46431c9_f473_33bc_a521_873ebb1d0854, 4, false) ) && <java:org.xenei.jdbc4sparql.sparql.CheckTypeF>(?v_01b7f5c9_44ab_3338_a788_6c35c335963f, 12, false) ) && <java:org.xenei.jdbc4sparql.sparql.CheckTypeF>(?v_caccfc47_d184_3d13_9990_16067ed22a39, 12, true) ) && <java:org.xenei.jdbc4sparql.sparql.CheckTypeF>(?v_270611a8_6b7e_3a2b_b560_4709cb515f5a, 4, true) )" + 
"      }" + 
"      BIND(<java:org.xenei.jdbc4sparql.sparql.ForceTypeF>(?v_caccfc47_d184_3d13_9990_16067ed22a39, 12, true) AS ?NullableStringCol)" + 
"      BIND(<java:org.xenei.jdbc4sparql.sparql.ForceTypeF>(?v_01b7f5c9_44ab_3338_a788_6c35c335963f, 12, false) AS ?type)" + 
"      BIND(<java:org.xenei.jdbc4sparql.sparql.ForceTypeF>(?v_270611a8_6b7e_3a2b_b560_4709cb515f5a, 4, true) AS ?NullableIntCol)" + 
"      BIND(<java:org.xenei.jdbc4sparql.sparql.ForceTypeF>(?v_c46431c9_f473_33bc_a521_873ebb1d0854, 4, false) AS ?IntCol)" + 
"      BIND(<java:org.xenei.jdbc4sparql.sparql.ForceTypeF>(?v_88b57d74_b9f7_3ae1_84d5_303f1b18058b, 12, false) AS ?StringCol)" + 
"    }" + 
"    FILTER ( ?StringCol = \"Foo2String\" )" + 
"  }" + 
"				";
		final Query query = QueryFactory.create(queryStr);

		//final Model model = dp.getMetaDatasetUnionModel();
		Field f = J4SStatement.class.getDeclaredField("catalog");
		f.setAccessible(true);
		Catalog c = (Catalog) f.get(stmt);
		List<QuerySolution> lst = c.executeLocalQuery(query);
		System.out.println( "yeah");
		
	}
	
}
