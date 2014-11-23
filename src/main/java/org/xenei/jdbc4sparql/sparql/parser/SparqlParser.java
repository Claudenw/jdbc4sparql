/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql.parser;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.lang.sparql_11.SPARQLParser11;
import com.hp.hpl.jena.sparql.syntax.Element;

import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.discovery.ResourceClassIterator;
import org.apache.commons.discovery.ResourceNameIterator;
import org.apache.commons.discovery.resource.ClassLoaders;
import org.apache.commons.discovery.resource.classes.DiscoverClasses;
import org.apache.commons.discovery.resource.names.DiscoverServiceNames;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

/**
 * An interface that defines the SparqlParser.
 * <p>
 * The sparql parser converts SQL to a SparqlQueryBuilder.
 * </p>
 * <p>
 * Must have a no argument constructor
 * </p>
 * <p>
 * It is conceivable that different implementations of the parser may be
 * required for different version of SQL.
 * </p>
 * <p>
 * Implementations of this interface should be listed in the
 * META-INF/services/org.xenei.jdbc4sparql.sparql.parser.SparqlBuilder file. The
 * first implementation listed in that file will be the default parser.
 * </p>
 * <p>
 * Implementations should implement <code>
 * public static final String PARSER_NAME</code> and <code>
 * public static final String DESCRIPTION </code>
 * </p>
 * <p>
 * if the PARSER_NAME is not specified the simple class name will be used.
 * </p>
 * <p>
 * if two builders have the same name only the first one will be seen.
 * </p>
 * <p>
 * A list of registered SparqlParsers is returned from J4SDriver when it is run
 * as a java application (e.g. java -jar J4DDriver.jar J4SDriver)
 * </p>
 */
public interface SparqlParser {
	/**
	 * A utility class that correctly handles common operations and provides
	 * common constants.
	 */
	static class Util {
		/**
		 * Get the defulat parser.
		 *
		 * @return SparqlParser
		 */
		public static SparqlParser getDefaultParser() {
			final List<Class<? extends SparqlParser>> lst = Util.getParsers();
			if (lst.size() == 0) {
				throw new IllegalStateException("No default parser defined");
			}
			try {
				return lst.get(0).newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(lst.get(0)
						+ " could not be instantiated.", e);
			}
		}

		/**
		 * Get the parser by parser name.
		 *
		 * The name may be a parser name or a fully qualified class name.
		 *
		 * If a class name is provided the class must implement SparqlParser.
		 *
		 * If the name is null the default parser is returned.
		 *
		 * @param parserName
		 *            The name to find.
		 * @return SparqlParser
		 */
		public static SparqlParser getParser(final String parserName) {
			if (parserName == null) {
				return Util.getDefaultParser();
			}

			try {
				final Class<?> clazz = Class.forName(parserName);
				if (SparqlParser.class.isAssignableFrom(clazz)) {
					try {
						return (SparqlParser) clazz.newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						throw new IllegalStateException(clazz
								+ " could not be instantiated.", e);
					}
				} else {
					throw new IllegalArgumentException(clazz
							+ " does not implement SparqlParser.");
				}
			} catch (final ClassNotFoundException e) {
				throw new IllegalArgumentException(parserName
						+ " is not a vaild SparqlParser");
			}
		}

		/**
		 * Get the list of parsers.
		 *
		 * @return The list of parsers.
		 */
		public static List<Class<? extends SparqlParser>> getParsers() {
			final List<Class<? extends SparqlParser>> retval = new ArrayList<Class<? extends SparqlParser>>();

			final ClassLoaders loaders = ClassLoaders.getAppLoaders(
					SparqlParser.class, SparqlParser.class, false);
			final DiscoverClasses<SparqlParser> dc = new DiscoverClasses<SparqlParser>(
					loaders);

			final ResourceNameIterator classIter = (new DiscoverServiceNames(
					loaders)).findResourceNames(SparqlParser.class.getName());
			final List<String> lst = new ArrayList<String>();

			// we build a list first because the classes can be found by
			// multiple loaders.
			while (classIter.hasNext()) {
				final String className = classIter.nextResourceName();
				if (!lst.contains(className)) {
					lst.add(className);
				}
			}
			// now just load the classes once.
			for (final String className : lst) {
				final ResourceClassIterator<SparqlParser> iter = dc
						.findResourceClasses(className);
				while (iter.hasNext()) {
					final Class<? extends SparqlParser> clazz = iter
							.nextResourceClass().loadClass();
					if (!retval.contains(clazz)) {
						retval.add(clazz);
					}
				}
			}
			// return the list
			return retval;
		}

		/**
		 * Parse a where clause into an element.
		 *
		 * @param qry
		 *            The query.
		 * @return The element
		 * @throws ParseException
		 *             on error.
		 */
		public static Element parse(final String qstr) throws ParseException,
				QueryException {

			final Query query = new Query();

			final Reader in = new StringReader(qstr);
			final SPARQLParser11 parser = new SPARQLParser11(in);
			query.setStrict(true);
			parser.setQuery(query);
			parser.WhereClause();

			return query.getQueryPattern();
		}
	}

	List<String> getSupportedNumericFunctions();

	List<String> getSupportedStringFunctions();

	List<String> getSupportedSystemFunctions();

	/**
	 * Parse the SQL string and then deparse it back into SQL to provide the SQL
	 * string native to the parser. This is used in support of nativeSQL() in
	 * the Driver.
	 *
	 * @param sqlQuery
	 *            the original SQL string
	 * @return the native SQL string
	 * @throws SQLException
	 *             on error.
	 */
	String nativeSQL(String sqlQuery) throws SQLException;

	/**
	 * Given a catalog and a SQL query create a SparqlQueryBuilder.
	 *
	 * @param catalog
	 *            The catalog to run the query against.
	 * @param sqlQuery
	 *            The SQL query to execute
	 * @return The SparqlQueryBuilder.
	 * @throws SQLException
	 */
	SparqlQueryBuilder parse(Map<String, Catalog> catalogs, Catalog catalog,
			Schema schema, String sqlQuery) throws SQLException;

}
