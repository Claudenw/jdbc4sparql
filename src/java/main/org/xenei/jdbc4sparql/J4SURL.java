package org.xenei.jdbc4sparql;

import java.net.URI;
import java.net.URISyntaxException;

public class J4SURL
{
	/**
	 * URLs of the form
	 * jdbc:J4S:<configlocation>
	 * jdbc:J4S?catalog=cat:<configlocation>
	 * jdbc:J4S?catalog=cat&builder=schema_builder:<sparqlendpint>
	 */
	public static final String SUB_PROTOCOL="J4S";
	public static final String CATALOG="catalog";
	public static final String BUILDER="builder";
	
	private URI endpoint;
	private String catalog;
	private String builder;
	
	public J4SURL( String urlStr )
	{
		String jdbc = "jdbc:";
		int pos = 0;
		if ( ! doComp( urlStr, pos, jdbc ))
		{
			throw new IllegalArgumentException( "Not a JDBC URL" );
		}
		pos += jdbc.length();
		if ( ! doComp( urlStr, pos, SUB_PROTOCOL ))
		{
			throw new IllegalArgumentException( "Not a J4S JDBC URL");
		}
		pos += SUB_PROTOCOL.length();
		if (urlStr.charAt(pos)=='?')
		{
			parseJ4SArgs( urlStr, pos+1 );
		}
		else if (urlStr.charAt(pos)==':')
		{
			parseJ4SEndpoint( urlStr, pos+1 );
		}
		else
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- missing endpoint");
		}
	}
	
	private String extractArg( String urlStr, int startPos )
	{
		if (urlStr.charAt(startPos) != '=')
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- argument must be followd by an equal sign '='" );
		}
		startPos++;
		int endPos = Integer.MAX_VALUE;
		
		int endPosCk = urlStr.indexOf(':', startPos);
		if (endPosCk != -1)
		{
			endPos = endPosCk;
		}
		
		endPosCk = urlStr.indexOf('&', startPos);
		if (endPosCk != -1)
		{
			endPos = Math.min( endPosCk,  endPos);
		}
		if (endPos == Integer.MAX_VALUE)
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- argument value must be followd by ':' or '&'" );
		}
		return urlStr.substring( startPos, endPos );
	}
	
	// parse the ?catalog=<x>&schema=<y>: as well as the ?catalog=<x>: versions
	private void parseJ4SArgs( String urlStr, int startPos )
	{
		int pos = startPos;
		if ( ! doComp( urlStr, pos, CATALOG))
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- '"+CATALOG+"' must be first argument" );
		}
		pos += CATALOG.length();
		this.catalog=extractArg( urlStr, pos );
		pos += 1+this.catalog.length();
		if (urlStr.charAt(pos) == '&')
		{
			pos++;
			if ( ! doComp( urlStr, pos, BUILDER))
			{
				throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- '"+BUILDER+"' is the only allowable second argument" );
			}
			pos += BUILDER.length();
			this.builder = extractArg(  urlStr, pos );
			pos += 1+this.builder.length();
		}
		if (! (urlStr.charAt(pos) == ':'))
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- arguments must be followed by a colon ':'" );
		}			
		parseJ4SEndpoint( urlStr, pos+1 );
	}
	
	private boolean doComp( String target, int pos, String comp )
	{
		String s = target.substring( pos, pos+comp.length() );
		return pos+comp.length()<target.length() && target.substring( pos, pos+comp.length() ).equalsIgnoreCase(comp);
	}
	
	private void parseJ4SEndpoint( String urlStr, int pos)
	{
		try
		{
			this.endpoint = new URI( urlStr.substring( pos ));
		}
		catch (URISyntaxException e)
		{
			throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- endpoint is not a valid URI : "+e.toString());
		}
	}

	public URI getEndpoint()
	{
		return endpoint;
	}

	public String getCatalog()
	{
		return catalog;
	}

	public String getBuilder()
	{
		return builder;
	}

}
