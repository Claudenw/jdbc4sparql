package org.xenei.jdbc4sparql.iface;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;

public abstract class TypeConverter
{

	private static final Object[][] typeMap =
		{
		{Types.CHAR, String.class},
		{Types.VARCHAR, String.class},
		{Types.NUMERIC, BigDecimal.class},
		{Types.DECIMAL, BigDecimal.class},
		{Types.BIT, Boolean.class},
		{Types.TINYINT,Byte.class},
		{Types.SMALLINT,Short.class},
		{Types.INTEGER,Integer.class},
		{Types.BIGINT,Long.class},
		{Types.REAL,Float.class},
		{Types.FLOAT,Double.class},
		{Types.DOUBLE,Double.class},
		{Types.DATE,java.sql.Date.class},
		{Types.TIME,java.sql.Time.class},
		{Types.TIMESTAMP,java.sql.Timestamp.class},
		{Types.BLOB,byte[].class},
		{Types.CLOB,char[].class},
		{Types.BINARY,byte[].class},
		{Types.LONGNVARCHAR,byte[].class},
		{Types.LONGVARCHAR,byte[].class},
		{Types.BOOLEAN,Boolean.class},
		};
	
	
	public static Class<?> getJavaType( int sqlType )
	{
		for (Object[] map : typeMap)
		{
			if (map[0].equals(sqlType))
			{
				return (Class<?>)map[1];
			}
		}
		return null;
	}
	
	public static Integer getSqlType( Class<?> javaType )
	{
		for (Object[] map : typeMap)
		{
			if (map[1].equals(javaType))
			{
				return (Integer)map[0];
			}
		}
		return null;
	}

}
