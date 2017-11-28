package org.xenei.jdbc4sparql.impl;

import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.items.NamedObject;

import org.apache.jena.sparql.core.Var;

public class NameUtils {

	public static String createUUIDName() {
		return ("v_" + UUID.randomUUID().toString()).replace("-", "_");
	}

	public static String getCursorName(final Table t) {
		return NameUtils.getCursorName(t.getName());
	}

	public static String getCursorName(final TableName name) {
		return "CURSOR_" + name.createName("_");
	}

	public static String getDBName(final NamedObject<?> namedObject) {
		return namedObject.getName().getDBName();
	}

	public static String getDBName(final Var var) {
		return var.getName().replace(NameUtils.SPARQL_DOT, NameUtils.DB_DOT);
	}

	public static String getSPARQLName(final NamedObject<?> namedObject) {
		return namedObject.getName().getSPARQLName( namedObject.getName().getDefaultSegments());
	}

	public static final String DB_DOT = ".";

	public static final String SPARQL_DOT = "\u00B7";

	public static final String[] DOT_LIST = {
			DB_DOT, SPARQL_DOT
	};

}
