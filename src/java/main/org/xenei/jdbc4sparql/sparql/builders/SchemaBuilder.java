package org.xenei.jdbc4sparql.sparql.builders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.discovery.ResourceClassIterator;
import org.apache.commons.discovery.ResourceIterator;
import org.apache.commons.discovery.ResourceNameIterator;
import org.apache.commons.discovery.resource.ClassLoaders;
import org.apache.commons.discovery.resource.DiscoverResources;
import org.apache.commons.discovery.resource.classes.DiscoverClasses;
import org.apache.commons.discovery.resource.names.DiscoverServiceNames;
import org.apache.commons.discovery.tools.DiscoverClass;
import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;

/**
 * An interface that defines classes that builds schemas.
 * <p>
 * Implementations of this interface should be listed in the 
 * META-INF/services/org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder
 * file.  The first implementation listed in that file will be the default
 * schema builder.
 * </p><p>
 * Implementations should implement <code>
 * public static final String BUILDER_NAME</code>
 * and <code>
 * public static final String DESCRIPTION </code>
 * </p><p>
 * if the BUILDER_NAME is not specified the simple class name will be used.  This is the 
 * name for the builder used in the J4S URL.  This means that all characters used in the name
 * should be easily typable into a URL without necessitating encoding.  However, this is not 
 * a hard requirement and is not enforced.
 * </p><p>
 * if two builders have the same name only the first one will be seen.
 * </p><p>
 * A list of registered SchemaBuilders is returned from J4SDriver when it is run as 
 * a java application (e.g. java -jar J4DDriver.jar J4SDriver)
 * </p>
 */
public interface SchemaBuilder
{
	Set<TableDef> getTableDefs( final SparqlCatalog catalog );
	
	public static class Util {
		
		public static List<Class<? extends SchemaBuilder>> getBuilders()
		{
			List<Class<? extends SchemaBuilder>> retval = new ArrayList<Class<? extends SchemaBuilder>>();
			
			ClassLoaders loaders = ClassLoaders.getAppLoaders( SchemaBuilder.class, SchemaBuilder.class, false );
			DiscoverClasses<SchemaBuilder> dc = new DiscoverClasses<SchemaBuilder>( loaders );

			 ResourceNameIterator classIter =
		                (new DiscoverServiceNames(loaders)).findResourceNames(SchemaBuilder.class.getName());
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
				 ResourceClassIterator<SchemaBuilder> iter = dc.findResourceClasses(className );
				 while (iter.hasNext())
				 {
					 Class<? extends SchemaBuilder> clazz = iter.nextResourceClass().loadClass();
					 if (!retval.contains( clazz)) {
						 retval.add( clazz );
					 }
				 }
				}
			 // return the list
			 return retval;
			 
		}

		public static String getName( Class<? extends SchemaBuilder> clazz)
		{
			return getField( clazz, "BUILDER_NAME", clazz.getSimpleName());
		}
		
		public static String getDescription( Class<? extends SchemaBuilder> clazz)
		{
			return getField( clazz, "DESCRIPTION", clazz.getName());
		}
		
		private static String getField(Class<? extends SchemaBuilder> clazz, String fieldName, String defaultValue)
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
		
		public static SchemaBuilder getBuilder(String name)
		{
			for (Class<? extends SchemaBuilder> c : getBuilders())
			{
				if (getName( c ).equals(name))
				{
					try
					{
						return c.newInstance();
					}
					catch (InstantiationException | IllegalAccessException e)
					{
						throw new IllegalStateException( c+" could not be instantiated.", e);
					}
				}
			}
			throw new IllegalArgumentException( "Unknown schema builder: "+name );
		}
	}
}
