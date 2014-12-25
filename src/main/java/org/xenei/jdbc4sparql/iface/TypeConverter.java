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
package org.xenei.jdbc4sparql.iface;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.apache.commons.io.IOUtils;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.datatypes.xsd.XSDDuration;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.graph.impl.LiteralLabelFactory;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.sparql.expr.NodeValue;

/**
 * Class to convert to and from SQL, SPARQL and Java types.
 *
 */
public final class TypeConverter {
	/**
	 * Don't instantiate.
	 */
	private TypeConverter() {
	}

	public static NodeValue getNodeValue(final Object value) {
		// add NodeValue types to SPARQLToJava class
		// so building a node value is easier.
		if (value == null) {
			return null;
		}
		final SPARQLToJava stj = getSPARQLType(value.getClass());
		final RDFDatatype dt = TypeMapper.getInstance().getTypeByName(
				stj.sparqlType);
		LiteralLabel lit = null;
		Node node = null;
		if (dt == null) {
			node = NodeFactory.createLiteral(value.toString());
		}
		else {
			lit = LiteralLabelFactory.create(value, null, dt);
			node = NodeFactory.createLiteral(lit);
		}
		return NodeValue.makeNode(node);
	}

	/**
	 * Get the java type from the sql type.
	 *
	 * @param sqlType
	 *            The sql type to lookup.
	 * @return The java class for the type.
	 * @throws SQLDataException
	 */
	public static Class<?> getJavaType(final int sqlType)
			throws SQLDataException {
		for (final SQLToJava map : TypeConverter.SQL_TO_JAVA) {
			if (map.sqlType == sqlType) {
				return map.javaType;
			}
		}
		throw new SQLDataException(String.format("SQL Type %s", sqlType));
	}

	/**
	 * Get the java type from the SPQRQL literal.
	 *
	 * @param literal
	 *            The SPARQL literal.
	 * @return the java class for the literal.
	 * @throws SQLDataException
	 */
	public static Class<?> getJavaType(final Literal literal)
			throws SQLDataException {
		return getJavaType(literal.getDatatype());
	}

	public static Class<?> getJavaType(final RDFDatatype dataType)
			throws SQLDataException {
		if (dataType == null) {
			return String.class;
		}
		for (final SPARQLToJava map : TypeConverter.SPARQL_TO_JAVA) {
			if (map.sparqlType.equals(dataType.getURI())) {
				return map.javaType;
			}
		}
		throw new SQLDataException(String.format("SPARQL Type %s",
				dataType.getURI()));
	}

	/**
	 * Get the java object for the literal value.
	 *
	 * @param literal
	 *            The literal to look up.
	 * @return The object.
	 */
	public static Object getJavaValue(final Literal literal) {
		final RDFDatatype dataType = literal.getDatatype();
		if (dataType == null) {
			return literal.toString();
		}
		return dataType.parse(literal.getLexicalForm());
	}

	/**
	 * Get the sql type for the java class.
	 *
	 * @param javaType
	 *            the java class to lookup.
	 * @return The sql Type for the class.
	 */
	public static Integer getSqlType(final Class<?> javaType) {
		for (final SQLToJava map : TypeConverter.SQL_TO_JAVA) {
			if (map.javaType.equals(javaType)) {
				return map.sqlType;
			}
		}
		return null;
	}

	private static SPARQLToJava getSPARQLType(final Class<?> javaType) {
		for (final SPARQLToJava map : TypeConverter.SPARQL_TO_JAVA) {
			if (map.javaType.equals(javaType)) {
				return map;
			}
		}
		return null;
	}

	/**
	 * True if the sql type is a boolean.
	 *
	 * @param sqlType
	 *            the sql type.
	 * @return true or false.
	 * @throws SQLDataException
	 */
	public static boolean isBoolean(final int sqlType) throws SQLDataException {
		return TypeConverter.getJavaType(sqlType).equals(Boolean.class);
	}

	/**
	 * True if the sql type is a date type
	 *
	 * @param sqlType
	 *            the sql type.
	 * @return true or false.
	 * @throws SQLDataException
	 */
	public static boolean isDate(final int sqlType) throws SQLDataException {
		return TypeConverter.getJavaType(sqlType).equals(java.sql.Date.class);
	}

	/**
	 * True if the sqlType is numeric.
	 *
	 * @param sqlType
	 *            the sql type
	 * @return true or false
	 * @throws SQLDataException
	 */
	public static boolean isNumeric(final int sqlType) throws SQLDataException {
		return Number.class
				.isAssignableFrom(TypeConverter.getJavaType(sqlType));
	}

	/**
	 * True if the sqlType is a time type.
	 *
	 * @param sqlType
	 * @return true or false
	 * @throws SQLDataException
	 */
	public static boolean isTime(final int sqlType) throws SQLDataException {
		return TypeConverter.getJavaType(sqlType).equals(java.sql.Date.class);
	}

	/**
	 * True if the sqlType is a timestamp type.
	 *
	 * @param sqlType
	 * @return true or false.
	 * @throws SQLDataException
	 */
	public static boolean isTimeStamp(final int sqlType)
			throws SQLDataException {
		return TypeConverter.getJavaType(sqlType).equals(
				java.sql.Timestamp.class);
	}

	/**
	 * The map of sqltype to java type
	 */
	private static class SQLToJava {
		int sqlType;
		Class<?> javaType;

		SQLToJava(final int sqlType, final Class<?> javaType) {
			this.sqlType = sqlType;
			this.javaType = javaType;
		}
	}

	// order is important. First java class instance found will be used to
	// convert
	// from java class to SQL class.
	private static final SQLToJava[] SQL_TO_JAVA = {
		// convert types
		new SQLToJava(Types.VARCHAR, String.class),
		new SQLToJava(Types.NUMERIC, BigDecimal.class),
		new SQLToJava(Types.BOOLEAN, Boolean.class),
		new SQLToJava(Types.TINYINT, Byte.class),
		new SQLToJava(Types.SMALLINT, Short.class),
		new SQLToJava(Types.INTEGER, Integer.class),
		new SQLToJava(Types.BIGINT, Long.class),
		new SQLToJava(Types.REAL, Float.class),
		new SQLToJava(Types.DOUBLE, Double.class),
		new SQLToJava(Types.DATE, java.sql.Date.class),
		new SQLToJava(Types.TIME, java.sql.Time.class),
		new SQLToJava(Types.TIMESTAMP, java.sql.Timestamp.class),
		new SQLToJava(Types.BLOB, byte[].class),
		new SQLToJava(Types.CLOB, char[].class),

		// duplicate types
		new SQLToJava(Types.BIT, Boolean.class),
		new SQLToJava(Types.BINARY, byte[].class),
		new SQLToJava(Types.LONGNVARCHAR, byte[].class),
		new SQLToJava(Types.LONGVARCHAR, byte[].class),
		new SQLToJava(Types.DECIMAL, BigDecimal.class),
		new SQLToJava(Types.FLOAT, Double.class),
		new SQLToJava(Types.CHAR, String.class),
	};

	/**
	 * The map of sparqltype to java type
	 */
	private static class SPARQLToJava {
		String sparqlType;
		Class<?> javaType;

		SPARQLToJava(final String sparqlType, final Class<?> javaType) {
			this.sparqlType = sparqlType;
			this.javaType = javaType;
		}
	}

	// order is important. First java class instance found will be used to
	// convert
	// from java class to SPARQL class.
	private static final SPARQLToJava[] SPARQL_TO_JAVA = {
		// convert types
		new SPARQLToJava(
				"http://www.w3.org/2001/XMLSchema#normalizedString",
				String.class),
				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#integer",
						BigInteger.class),
						new SPARQLToJava("http://www.w3.org/2001/XMLSchema#short",
								Short.class),
								new SPARQLToJava("http://www.w3.org/2001/XMLSchema#time",
										Time.class),
										new SPARQLToJava("http://www.w3.org/2001/XMLSchema#float",
												Float.class),
												new SPARQLToJava("http://www.w3.org/2001/XMLSchema#base64Binary",
														byte[].class),
														new SPARQLToJava("http://www.w3.org/2001/XMLSchema#double",
																Double.class),
																new SPARQLToJava("http://www.w3.org/2001/XMLSchema#dateTime",
																		XSDDateTime.class),
																		new SPARQLToJava("http://www.w3.org/2001/XMLSchema#byte",
																				Byte.class),
																				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#anyURI",
																						URI.class),
																						new SPARQLToJava("http://www.w3.org/2001/XMLSchema#duration",
																								XSDDuration.class),
																								new SPARQLToJava("http://www.w3.org/2001/XMLSchema#int",
																										Integer.class),
																										new SPARQLToJava("http://www.w3.org/2001/XMLSchema#boolean",
																												Boolean.class),
																												new SPARQLToJava("http://www.w3.org/2001/XMLSchema#decimal",
																														BigDecimal.class),
																														new SPARQLToJava("http://www.w3.org/2001/XMLSchema#long",
																																Long.class),

																																// duplicate types
																																new SPARQLToJava("http://www.w3.org/2001/XMLSchema#NMTOKEN",
																																		String.class),
																																		new SPARQLToJava("http://www.w3.org/2001/XMLSchema#unsignedInt",
																																				String.class),
																																				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#unsignedShort",
																																						String.class),
																																						new SPARQLToJava("http://www.w3.org/2001/XMLSchema#unsignedLong",
																																								String.class),
																																								new SPARQLToJava("http://www.w3.org/2001/XMLSchema#token",
																																										String.class),
																																										new SPARQLToJava("http://www.w3.org/2001/XMLSchema#NCName",
																																												String.class),
																																												new SPARQLToJava("http://www.w3.org/2001/XMLSchema#ID",
																																														String.class),
																																														new SPARQLToJava(
																																																"http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral",
																																																String.class),
																																																new SPARQLToJava("http://www.w3.org/2001/XMLSchema#anySimpleType",
																																																		String.class),
																																																		new SPARQLToJava("http://www.w3.org/2001/XMLSchema#gYear",
																																																				String.class),
																																																				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#string",
																																																						String.class),
																																																						new SPARQLToJava("http://www.w3.org/2001/XMLSchema#date",
																																																								String.class),
																																																								new SPARQLToJava("http://www.w3.org/2001/XMLSchema#gMonthDay",
																																																										String.class),
																																																										new SPARQLToJava("http://www.w3.org/2001/XMLSchema#unsignedByte",
																																																												String.class),
																																																												new SPARQLToJava(
																																																														"http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
																																																														String.class),
																																																														new SPARQLToJava("http://www.w3.org/2001/XMLSchema#QName",
																																																																String.class),
																																																																new SPARQLToJava("http://www.w3.org/2001/XMLSchema#Name",
																																																																		String.class),
																																																																		new SPARQLToJava(
																																																																				"http://www.w3.org/2001/XMLSchema#negativeInteger",
																																																																				String.class),
																																																																				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#hexBinary",
																																																																						String.class),
																																																																						new SPARQLToJava(
																																																																								"http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
																																																																								String.class),
																																																																								new SPARQLToJava("http://www.w3.org/2001/XMLSchema#NOTATION",
																																																																										String.class),
																																																																										new SPARQLToJava("http://www.w3.org/2001/XMLSchema#language",
																																																																												String.class),
																																																																												new SPARQLToJava(
																																																																														"http://www.w3.org/2001/XMLSchema#positiveInteger",
																																																																														String.class),
																																																																														new SPARQLToJava("http://www.w3.org/2001/XMLSchema#IDREF",
																																																																																String.class),
																																																																																new SPARQLToJava("http://www.w3.org/2001/XMLSchema#ENTITY",
																																																																																		String.class),
																																																																																		new SPARQLToJava("http://www.w3.org/2001/XMLSchema#gDay",
																																																																																				String.class),
																																																																																				new SPARQLToJava("http://www.w3.org/2001/XMLSchema#gMonth",
																																																																																						String.class),
																																																																																						new SPARQLToJava("http://www.w3.org/2001/XMLSchema#gYearMonth",
																																																																																								String.class),
	};

	private static Map<Class<?>, Object> nullValueMap;

	static {
		nullValueMap = new HashMap<Class<?>, Object>();
		nullValueMap.put(Boolean.class, Boolean.FALSE);
		nullValueMap.put(Byte.class, new Byte((byte) 0));
		nullValueMap.put(Short.class, new Short((short) 0));
		nullValueMap.put(Integer.class, new Integer(0));
		nullValueMap.put(Long.class, new Long(0L));
		nullValueMap.put(Float.class, new Float(0.0F));
		nullValueMap.put(Double.class, new Double(0.0));
	}

	@SuppressWarnings("unchecked")
	public static <T> T extractData(final Object columnObject,
			final Class<T> resultingClass) throws SQLException {
		if (columnObject == null) {
			return (T) nullValueMap.get(resultingClass);
		}

		// try the simple case
		if (resultingClass.isAssignableFrom(columnObject.getClass())) {
			return resultingClass.cast(columnObject);
		}

		// see if we can do a simple numeric assignment
		if (columnObject instanceof Number) {
			return fromNumber(columnObject, resultingClass);
		}

		// see if we can convert from a string
		if (columnObject instanceof String) {
			return fromString(columnObject, resultingClass);
		}

		if (columnObject instanceof Boolean) {
			final Boolean b = (Boolean) columnObject;
			return fromString(b ? "1" : "0", resultingClass);
		}

		if (columnObject instanceof byte[]) {
			try {
				if (resultingClass.isAssignableFrom(Clob.class)) {
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(new ByteArrayInputStream(
									(byte[]) columnObject))));
				}
				if (resultingClass.isAssignableFrom(Blob.class)) {
					return resultingClass.cast(new SerialBlob(
							(byte[]) columnObject));
				}
				if (resultingClass.isAssignableFrom(InputStream.class)) {
					return resultingClass.cast(new ByteArrayInputStream(
							(byte[]) columnObject));
				}
				final String s = new String((byte[]) columnObject);
				return fromString(s, resultingClass);
			} catch (final IOException e) {
				throw new SQLException(e.getMessage(), e);
			}
		}

		if (columnObject instanceof Blob) {
			try {
				final Blob b = (Blob) columnObject;
				if (resultingClass.isAssignableFrom(byte[].class)) {
					return resultingClass.cast(IOUtils.toByteArray(b
							.getBinaryStream()));
				}
				if (resultingClass.isAssignableFrom(Clob.class)) {
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(b.getBinaryStream())));
				}
				if (resultingClass.isAssignableFrom(InputStream.class)) {
					return resultingClass.cast(b.getBinaryStream());
				}
				final String s = new String(
						IOUtils.toByteArray(((Blob) columnObject)
								.getBinaryStream()));
				return fromString(s, resultingClass);
			} catch (final IOException e) {
				throw new SQLException(e.getMessage(), e);
			}
		}
		if (columnObject instanceof Clob) {
			try {
				final Clob c = (Clob) columnObject;
				if (resultingClass.isAssignableFrom(byte[].class)) {
					return resultingClass.cast(IOUtils.toByteArray(c
							.getAsciiStream()));
				}
				if (resultingClass.isAssignableFrom(Blob.class)) {
					return resultingClass.cast(new SerialBlob(IOUtils
							.toByteArray(c.getAsciiStream())));
				}
				if (resultingClass.isAssignableFrom(InputStream.class)) {
					return resultingClass.cast(c.getAsciiStream());
				}
				final String s = String.valueOf(IOUtils.toCharArray(c
						.getCharacterStream()));
				return fromString(s, resultingClass);
			} catch (final IOException e) {
				throw new SQLException(e.getMessage(), e);
			}
		}
		if (columnObject instanceof InputStream) {
			try {
				final InputStream is = (InputStream) columnObject;
				if (resultingClass.isAssignableFrom(Clob.class)) {
					return resultingClass.cast(new SerialClob(IOUtils
							.toCharArray(is)));
				}
				if (resultingClass.isAssignableFrom(Blob.class)) {
					return resultingClass.cast(new SerialBlob(IOUtils
							.toByteArray(is)));
				}
				if (resultingClass.isAssignableFrom(byte[].class)) {
					return resultingClass.cast(IOUtils.toByteArray(is));
				}
				return fromString(new String(IOUtils.toByteArray(is)),
						resultingClass);
			} catch (final IOException e) {
				throw new SQLException(e.getMessage(), e);
			}
		}
		throw new SQLException(String.format(" Can not cast %s (%s) to %s",
				columnObject.getClass(), columnObject.toString(),
				resultingClass));
	}

	private static <T> T fromNumber(final Object columnObject,
			final Class<T> resultingClass) throws SQLException {
		final Number n = Number.class.cast(columnObject);
		if (resultingClass == BigDecimal.class) {
			return resultingClass.cast(new BigDecimal(n.toString()));
		}
		if (resultingClass == BigInteger.class) {
			return resultingClass.cast(new BigInteger(n.toString()));
		}
		if (resultingClass == Byte.class) {
			return resultingClass.cast(new Byte(n.byteValue()));
		}
		if (resultingClass == Double.class) {
			return resultingClass.cast(new Double(n.doubleValue()));
		}
		if (resultingClass == Float.class) {
			return resultingClass.cast(new Float(n.floatValue()));
		}
		if (resultingClass == Integer.class) {
			return resultingClass.cast(new Integer(n.intValue()));
		}
		if (resultingClass == Long.class) {
			return resultingClass.cast(new Long(n.longValue()));
		}
		if (resultingClass == Short.class) {
			return resultingClass.cast(new Short(n.shortValue()));
		}
		if (resultingClass == String.class) {
			return resultingClass.cast(n.toString());
		}
		if (resultingClass == Boolean.class) {
			if (n.byteValue() == 0) {
				return resultingClass.cast(Boolean.FALSE);
			}
			if (n.byteValue() == 1) {
				return resultingClass.cast(Boolean.TRUE);
			}
		}
		if (resultingClass == byte[].class) {
			return resultingClass.cast(n.toString().getBytes());
		}
		if (resultingClass == Blob.class) {
			return resultingClass.cast(new SerialBlob(n.toString().getBytes()));
		}
		if (resultingClass == Clob.class) {
			return resultingClass.cast(new SerialClob(n.toString()
					.toCharArray()));
		}
		return null;
	}

	private static <T> T fromString(final Object columnObject,
			final Class<T> resultingClass) throws SQLException {
		final String val = String.class.cast(columnObject);
		// to numeric casts
		try {
			if (resultingClass == BigDecimal.class) {
				return resultingClass.cast(new BigDecimal(val));
			}
			if (resultingClass == BigInteger.class) {
				return resultingClass.cast(new BigInteger(val));
			}
			if (resultingClass == Byte.class) {
				return resultingClass.cast(new Byte(val));
			}
			if (resultingClass == Double.class) {
				return resultingClass.cast(new Double(val));
			}
			if (resultingClass == Float.class) {
				return resultingClass.cast(new Float(val));
			}
			if (resultingClass == Integer.class) {
				return resultingClass.cast(new Integer(val));
			}
			if (resultingClass == Long.class) {
				return resultingClass.cast(new Long(val));
			}
			if (resultingClass == Short.class) {
				return resultingClass.cast(new Short(val));
			}
		} catch (final NumberFormatException e) {
			return null;
		}

		if (resultingClass == Boolean.class) {
			if ("0".equals(val)) {
				return resultingClass.cast(Boolean.FALSE);
			}
			if ("1".equals(val)) {
				return resultingClass.cast(Boolean.TRUE);
			}
		}
		if (resultingClass == byte[].class) {
			return resultingClass.cast(val.getBytes());
		}
		if (resultingClass == Blob.class) {
			return resultingClass.cast(new SerialBlob(val.getBytes()));
		}
		if (resultingClass == Clob.class) {
			return resultingClass.cast(new SerialClob(val.toCharArray()));
		}
		return null;
	}

}
