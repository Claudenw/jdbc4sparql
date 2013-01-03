package org.xenei.jdbc4sparql.sparql.parser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public interface SparqlParser
{
	static class Util
	{
		static final int START = 0;
		static final int END = 1;
		static final String[] URI_MARKERS = { "<", ">" };
		static final String[] QUOT_MARKERS = { "\"'", "\"'" };
		static final String[] BLANK_MARKERS = { "[", "]" };
		static final String[] VAR_MARKERS = { "?$", " " };

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
				/*
				 * else if (VAR_MARKERS[START].indexOf(buffer.charAt(i))>-1)
				 * {
				 * int start = i;
				 * while (VAR_MARKERS[END].indexOf(buffer.charAt(i))==-1)
				 * {
				 * i++;
				 * if (i==buffer.length()){
				 * throw new IllegalArgumentException(
				 * buffer+" is missig a closing variable marker");
				 * }
				 * }
				 * results.add( buffer.substring( start, i));
				 * }
				 */
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

	}

	public static final String SPARQL_DOT = "\u00B7";

	SparqlQueryBuilder parse( String sqlQuery ) throws SQLException;

}
