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
package org.xenei.jdbc4sparql;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;
import org.xenei.jdbc4sparql.utils.SQLNameUtil;

/**
 * The J4S URL.
 *
 * URLs of the form jdbc:J4S:<configlocation>
 * jdbc:J4S?catalog=cat:<configlocation>
 * jdbc:J4S?catalog=cat&builder=builderclass:<sparqlendpint>
 */
public class J4SUrl {
  
    public static final String SUB_PROTOCOL = "J4S";
    public static final String TYPE_SPARQL = "sparql";
    public static final String TYPE_CONFIG = "config";
    public static final String[] ARGS = { J4SPropertyNames.CATALOG_PROPERTY, J4SPropertyNames.TYPE_PROPERTY,
            J4SPropertyNames.BUILDER_PROPERTY, J4SPropertyNames.PARSER_PROPERTY };

    private URI endpoint;
    private SparqlParser parser;
    private SchemaBuilder builder;
    private final Properties properties;

    /**
     * Constructor.
     * Parses are URL of the form jdbc:j4s[?ARG=Val[&ARG=VAL...]]:[URI]
     *
     * @param urlStr The string to parse as a URL
     */
    public J4SUrl(final String urlStr) {
        this.properties = new Properties();
        final String jdbc = "jdbc:";
        int pos = 0;
        if (!doComp( urlStr, pos, jdbc )) {
            throw new IllegalArgumentException( "Not a JDBC URL" );
        }
        pos += jdbc.length();
        if (!doComp( urlStr, pos, J4SUrl.SUB_PROTOCOL )) {
            throw new IllegalArgumentException( "Not a J4S JDBC URL" );
        }
        pos += J4SUrl.SUB_PROTOCOL.length();
        if (urlStr.charAt( pos ) == '?') {
            parseJ4SArgs( urlStr, pos + 1 );
        } else if (urlStr.charAt( pos ) == ':') {
            parseJ4SEndpoint( urlStr, pos + 1 );
        } else {
            throw new IllegalArgumentException( "Not a valid J4S JDBC URL -- missing endpoint" );
        }

        // configure catalog name
        if (StringUtils.isBlank( getCatalog() )) {
            String catalogName = getEndpoint().getHost();
            if (!StringUtils.isBlank( catalogName )) {
                catalogName = SQLNameUtil.clean( catalogName );
                properties.setProperty( J4SPropertyNames.CATALOG_PROPERTY, catalogName );
            }
        }
    }

    private boolean doComp(final String target, final int pos, final String comp) {
        target.substring( pos, pos + comp.length() );
        return ((pos + comp.length()) < target.length())
                && target.substring( pos, pos + comp.length() ).equalsIgnoreCase( comp );
    }

    /**
     * Get the schema builder for this URL.
     *
     * @return the SchemaBuilder
     */
    public SchemaBuilder getBuilder() {
        return builder;
    }

    /**
     * Get the default catalog for the URL.
     *
     * @return the default catalog name or build one from URL name
     */
    public String getCatalog() {
        return properties.getProperty( J4SPropertyNames.CATALOG_PROPERTY, "" );
    }

    /**
     * Get the endpoint for the URL.
     *
     * The endpoint may be a local file or a sparql endpoint.
     *
     * @return the URI for the endpoint.
     */
    public URI getEndpoint() {
        return endpoint;
    }

    /**
     * Get the language for the specified type. Will return null for SPARQL and
     * CONFIG
     *
     * @return
     */
    public Lang getLang() {
        return RDFLanguages.nameToLang( getType() );
    }

    /**
     * Get the sparql parser for the URL.
     *
     * The sparlq parser converts SQL to SPARQL.
     *
     * @return the sparql parser.
     */
    public SparqlParser getParser() {
        return parser;
    }

    /**
     * Get the properties specified in the URL.
     *
     * @return The properties.
     */
    public Properties getProperties() {
        return new Properties( properties );
    }

    /**
     * Get the type (format) of the endpoint URI.
     *
     * May be Sparql, Config, or one of the language types supported by Apache Jena.
     * e.g. TTL, RDF/XML, N3, etc.
     *
     * @return The type of the endpoint URI.
     */
    public String getType() {
        return properties.getProperty( J4SPropertyNames.TYPE_PROPERTY, J4SUrl.TYPE_CONFIG );
    }

    // parse the ?catalog=<x>&schema=<y>: as well as the ?catalog=<x>: versions
    /**
     * Parse an argument out of the URL string section.
     *
     * Should be of the form x=y[:|&]
     *
     * @param urlStr
     * @param startPos
     */
    private void parseJ4SArgs(final String urlStr, final int startPos) {

        int pos = startPos;
        // (arg)=(val)(:|&)
        final Pattern pattern = Pattern.compile( "(([a-zA-Z]+)(\\=([^:\\&]+))?([:|\\&])).+" );
        Matcher matcher = pattern.matcher( urlStr.substring( startPos ) );

        while (matcher.matches()) {
            final String arg = matcher.group( 2 );
            boolean found = false;
            for (final String validArg : J4SUrl.ARGS) {
                found |= validArg.equalsIgnoreCase( arg );
            }
            if (!found) {
                throw new IllegalArgumentException(
                        "Not a valid J4S JDBC URL -- '" + arg + "' is not a recognized argument" );
            }
            properties.put( arg, StringUtils.defaultIfBlank( matcher.group( 4 ), "" ) );
            pos += matcher.group( 1 ).length();
            if (":".equals( matcher.group( 5 ) )) {
                matcher = pattern.matcher( "" );
            } else {
                matcher = pattern.matcher( urlStr.substring( pos ) );
            }
        }

        // check for valid type value and make sure it is upper case.
        // valid type is a Jena Lang.
        if (properties.containsKey( J4SPropertyNames.TYPE_PROPERTY )) {
            final String type = properties.getProperty( J4SPropertyNames.TYPE_PROPERTY );
            if (type.equalsIgnoreCase( J4SUrl.TYPE_SPARQL )) {
                properties.setProperty( J4SPropertyNames.TYPE_PROPERTY, J4SUrl.TYPE_SPARQL );
            } else {
                final Lang l = RDFLanguages.nameToLang( type );
                if (l != null) {
                    properties.setProperty( J4SPropertyNames.TYPE_PROPERTY, l.getName() );
                } else {
                    throw new IllegalArgumentException(
                            "Not a valid J4S JDBC URL -- '" + type + "' is not a recognized type value" );
                }
            }
        }
        if (properties.containsKey( J4SPropertyNames.PARSER_PROPERTY )) {
            // verify we can load the parser
            parser = SparqlParser.Util.getParser( properties.getProperty( J4SPropertyNames.PARSER_PROPERTY ) );
        }
        if (properties.containsKey( J4SPropertyNames.BUILDER_PROPERTY )) {
            // verify we can load the builder
            builder = SchemaBuilder.Util.getBuilder( properties.getProperty( J4SPropertyNames.BUILDER_PROPERTY ) );
        }
        parseJ4SEndpoint( urlStr, pos );
    }

    private void parseJ4SEndpoint(final String urlStr, final int pos) {
        try {
            this.endpoint = new URI( urlStr.substring( pos ) );
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Not a valid J4S JDBC URL -- endpoint is not a valid URI : " + e.toString() );
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder( "jdbc:" ).append( J4SUrl.SUB_PROTOCOL ).append( ":" );
        if (!properties.isEmpty()) {
            sb.append( "?" );
            final int limit = properties.keySet().size();
            int i = 0;
            for (final Object key : properties.keySet()) {
                sb.append( key.toString() ).append( "=" ).append( properties.getProperty( key.toString() ) );
                if (++i < limit) {
                    sb.append( "&" );
                }
            }
            sb.append( ":" );
        }
        sb.append( endpoint.normalize().toASCIIString() );
        return sb.toString();
    }
}
