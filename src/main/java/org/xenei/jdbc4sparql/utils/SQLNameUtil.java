package org.xenei.jdbc4sparql.utils;

public class SQLNameUtil
{

	private SQLNameUtil()
	{
		
	}
	
	public static String clean(String name)
	{
		return name.replaceAll("[^A-Za-z0-9]+", "_");
	}

}
