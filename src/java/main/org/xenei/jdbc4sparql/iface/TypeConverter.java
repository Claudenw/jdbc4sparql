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

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;

import java.math.BigDecimal;
import java.sql.Types;

public abstract class TypeConverter
{

	private static final Object[][] typeMap = { { Types.CHAR, String.class },
			{ Types.VARCHAR, String.class },
			{ Types.NUMERIC, BigDecimal.class },
			{ Types.DECIMAL, BigDecimal.class }, { Types.BIT, Boolean.class },
			{ Types.TINYINT, Byte.class }, { Types.SMALLINT, Short.class },
			{ Types.INTEGER, Integer.class }, { Types.BIGINT, Long.class },
			{ Types.REAL, Float.class }, { Types.FLOAT, Double.class },
			{ Types.DOUBLE, Double.class },
			{ Types.DATE, java.sql.Date.class },
			{ Types.TIME, java.sql.Time.class },
			{ Types.TIMESTAMP, java.sql.Timestamp.class },
			{ Types.BLOB, byte[].class }, { Types.CLOB, char[].class },
			{ Types.BINARY, byte[].class },
			{ Types.LONGNVARCHAR, byte[].class },
			{ Types.LONGVARCHAR, byte[].class },
			{ Types.BOOLEAN, Boolean.class }, };

	public static Class<?> getJavaType( final int sqlType )
	{
		for (final Object[] map : TypeConverter.typeMap)
		{
			if (map[0].equals(sqlType))
			{
				return (Class<?>) map[1];
			}
		}
		return null;
	}

	public static Object getJavaValue( final Literal literal )
	{
		final RDFDatatype dataType = literal.getDatatype();
		if (dataType == null)
		{
			return literal.toString();
		}
		return dataType.parse(literal.getLexicalForm());
	}

	public static Integer getSqlType( final Class<?> javaType )
	{
		for (final Object[] map : TypeConverter.typeMap)
		{
			if (map[1].equals(javaType))
			{
				return (Integer) map[0];
			}
		}
		return null;
	}

}
