package org.xenei.jdbc4sparql.sparql.parser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.discovery.ResourceClassIterator;
import org.apache.commons.discovery.ResourceNameIterator;
import org.apache.commons.discovery.resource.ClassLoaders;
import org.apache.commons.discovery.resource.classes.DiscoverClasses;
import org.apache.commons.discovery.resource.names.DiscoverServiceNames;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

/**
 * An interface that defines the SparqlParser.
 * <p>
 * The sparql parser converts SQL to a SparqlQueryBuilder.
 * </p><p>
 * Must have a no argument constructor
 * </p><p>
 * It is conceivable that different implementations of the parser may be required for different
 * version of SQL.
 * </p><p>
 * Implementations of this interface should be listed in the 
 * META-INF/services/org.xenei.jdbc4sparql.sparql.parser.SparqlBuilder
 * file.  The first implementation listed in that file will be the default
 * parser.
 * </p><p>
 * Implementations should implement <code>
 * public static final String PARSER_NAME</code>
 * and <code>
 * public static final String DESCRIPTION </code>
 * </p><p>
 * if the PARSER_NAME is not specified the simple class name will be used.  
 * </p><p>
 * if two builders have the same name only the first one will be seen.
 * </p><p>
 * A list of registered SparqlParsers is returned from J4SDriver when it is run as 
 * a java application (e.g. java -jar J4DDriver.jar J4SDriver)
 * </p>

 */
public interface SparqlParser
{
	static class Util
	{
		private static final int START = 0;
		private static final int END = 1;
		private static final String[] URI_MARKERS = { "<", ">" };
		private static final String[] QUOT_MARKERS = { "\"'", "\"'" };
		private static final String[] BLANK_MARKERS = { "[", "]" };
		private static final String[] VAR_MARKERS = { "?$", " " };

		public static Node parseNode( final String nodeStr )
		{
			final String s = nodeStr.trim();
			if (Util.URI_MARKERS[Util.START].contains(s.substring(0, 1)))
			{
				return Node.createURI(s.substring(1, s.length() - 1));
			}
			else if (Util.BLANK_MARKERS[Util.START].contains(s.substring(0, 1)))
			{
				return Node.createAnon();
			}
			else if (Util.VAR_MARKERS[Util.START].contains(s.substring(0, 1)))
			{
				return Var.alloc(Node.createVariable(s.substring(1)));
			}
			else if (Util.QUOT_MARKERS[Util.START].contains(s.substring(0, 1)))
			{
				return Node.createLiteral(s.substring(1, s.length() - 1));
			}
			return Node.createLiteral(s);
		}

		public static List<String> parseQuerySegment( final String segment )
		{
			final String buffer = segment.trim() + " "; // space for var
														// processing
			final List<String> results = new ArrayList<String>();
			int i = -1;
			while (i < (buffer.length() - 1))
			{
				i++;
				if (Util.BLANK_MARKERS[Util.START].indexOf(buffer.charAt(i)) > -1)
				{
					final int start = i;
					while (Util.BLANK_MARKERS[Util.END].indexOf(buffer
							.charAt(i)) == -1)
					{
						i++;
						if (i == buffer.length())
						{
							throw new IllegalArgumentException(buffer
									+ " is missig a closing blank marker");
						}
					}
					results.add(buffer.substring(start, i + 1));
				}
				else if (Util.URI_MARKERS[Util.START].indexOf(buffer.charAt(i)) > -1)
				{
					final int start = i;
					while (Util.URI_MARKERS[Util.END].indexOf(buffer.charAt(i)) == -1)
					{
						i++;
						if (i == buffer.length())
						{
							throw new IllegalArgumentException(buffer
									+ " is missig a closing uri marker");
						}
					}
					results.add(buffer.substring(start, i + 1));
				}
				else if (Util.QUOT_MARKERS[Util.START]
						.indexOf(buffer.charAt(i)) > -1)
				{
					final int start = i;
					final StringBuffer stack = new StringBuffer();
					stack.append(buffer.charAt(i));
					while (stack.length() > 0)
					{
						i++;
						if (i == buffer.length())
						{
							throw new IllegalArgumentException(buffer
									+ " is missig a closing quote marker ["
									+ stack.charAt(stack.length() - 1));
						}
						if (buffer.charAt(i) == stack
								.charAt(stack.length() - 1))
						{
							stack.deleteCharAt(stack.length() - 1);
						}
						else
						{
							if (Util.QUOT_MARKERS[Util.START].indexOf(buffer
									.charAt(i)) > -1)
							{
								stack.append(buffer.charAt(i));
							}
						}

					}
					results.add(buffer.substring(start, i + 1));
				}
				else
				{
					// must be a literal or a var
					if (buffer.charAt(i) != ' ')
					{
						final int start = i;
						i++;
						if (i == buffer.length())
						{
							throw new IllegalArgumentException(buffer
									+ " is missig a closing variable marker");
						}
						while (buffer.charAt(i) != ' ')
						{
							i++;
							if (i == buffer.length())
							{
								throw new IllegalArgumentException(
										buffer
												+ " is missig a closing variable marker");
							}
						}
						results.add(buffer.substring(start, i));
					}
				}

			}
			return results;
		}

		public static String SparqlDBName( final String dbName )
		{
			return dbName.replace(".", SparqlParser.SPARQL_DOT);
		}

		public static String UnSparqlDBName( final String sparqlDBName )
		{
			return sparqlDBName.replace(SparqlParser.SPARQL_DOT, ".");
		}

		public static List<Class<? extends SparqlParser>> getParsers()
		{
			List<Class<? extends SparqlParser>> retval = new ArrayList<Class<? extends SparqlParser>>();
			
			ClassLoaders loaders = ClassLoaders.getAppLoaders( SparqlParser.class, SparqlParser.class, false );
			DiscoverClasses<SparqlParser> dc = new DiscoverClasses<SparqlParser>( loaders );

			 ResourceNameIterator classIter =
		                (new DiscoverServiceNames(loaders)).findResourceNames(SparqlParser.class.getName());
			 List<String> lst = new ArrayList<String>();
			 
			 // we build a list first because the classes can be found by multiple loaders.
			 while (classIter.hasNext())
				{
				 String className = classIter.nextResourceName();
				 if (!lst.contains(className))
				 {
					 lst.add( className );
				 }
				}
			 // now just load the classes once.
			 for (String className : lst )
			 {
				 ResourceClassIterator<SparqlParser> iter = dc.findResourceClasses(className );
				 while (iter.hasNext())
				 {
					 Class<? extends SparqlParser> clazz = iter.nextResourceClass().loadClass();
					 if (!retval.contains( clazz)) {
						 retval.add( clazz );
					 }
				 }
				}
			 // return the list
			 return retval;
			 
		}

		public static String getName( Class<? extends SparqlParser> clazz)
		{
			return getField( clazz, "PARSER_NAME", clazz.getSimpleName());
		}
		
		public static String getDescription( Class<? extends SparqlParser> clazz)
		{
			return getField( clazz, "DESCRIPTION", clazz.getName());
		}
		
		private static String getField(Class<? extends SparqlParser> clazz, String fieldName, String defaultValue)
		{
			try
			{
				Field f = clazz.getField(fieldName);
				if (Modifier.isStatic(f.getModifiers()))
				{
					return f.get(null).toString();
				}
			}
			catch (NoSuchFieldException e)
			{
				// do nothing -- acceptable
			}
			catch (SecurityException e)
			{
				// do nothing -- acceptable
			}
			catch (IllegalArgumentException e)
			{
				// do nothing -- acceptable
			}
			catch (IllegalAccessException e)
			{
				// do nothing -- acceptable
			}
			return defaultValue;
		}
		
		public static SparqlParser getDefaultParser()
		{
			List<Class<? extends SparqlParser>> lst = getParsers();
			if (lst.size() == 0)
			{
				throw new IllegalStateException( "No default parser defined");
			}
				
			try
			{
				return lst.get(0).newInstance();
			}
			catch (InstantiationException | IllegalAccessException e)
			{
				throw new IllegalStateException( getName( lst.get(0))+" could not be instantiated.", e);
			}
		}
	}
	

	public static final String SPARQL_DOT = "\u00B7";

	SparqlQueryBuilder parse( SparqlCatalog catalog, String sqlQuery ) throws SQLException;
	
	/**
	 * Parse the SQL string and then deparse it back into SQL to provide
	 * the SQL string native to the parser.  This is used in support of 
	 * nativeSQL() in the Driver.
	 * @param sqlQuery the original SQL string
	 * @return the native SQL string
	 * @throws SQLException on error.
	 */
	String nativeSQL( String sqlQuery ) throws SQLException;

}
