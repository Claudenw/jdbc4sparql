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
package org.xenei.jdbc4sparql.sparql.builders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.discovery.ResourceClassIterator;
import org.apache.commons.discovery.ResourceNameIterator;
import org.apache.commons.discovery.resource.ClassLoaders;
import org.apache.commons.discovery.resource.classes.DiscoverClasses;
import org.apache.commons.discovery.resource.names.DiscoverServiceNames;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;

/**
 * An interface that defines classes that builds schemas.
 * <p>
 * Implementations of this interface should be listed in the
 * META-INF/services/org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder file.
 * The first implementation listed in that file will be the default schema
 * builder.
 * </p>
 * <p>
 * Implementations should implement <code>
 * public static final String BUILDER_NAME</code> and <code>
 * public static final String DESCRIPTION </code>
 * </p>
 * <p>
 * if the BUILDER_NAME is not specified the simple class name will be used. This
 * is the name for the builder used in the J4S URL. This means that all
 * characters used in the name should be easily typable into a URL without
 * necessitating encoding. However, this is not a hard requirement and is not
 * enforced.
 * </p>
 * <p>
 * if two builders have the same name only the first one will be seen.
 * </p>
 * <p>
 * A list of registered SchemaBuilders is returned from J4SDriver when it is run
 * as a java application (e.g. java -jar J4DDriver.jar J4SDriver)
 * </p>
 */
public interface SchemaBuilder
{
	public static class Util
	{

		public static SchemaBuilder getBuilder( final String name )
		{
			final List<Class<? extends SchemaBuilder>> lst = Util.getBuilders();
			if (name == null)
			{
				try
				{
					return lst.get(0).newInstance();
				}
				catch (InstantiationException | IllegalAccessException e)
				{
					throw new IllegalStateException(lst.get(0)
							+ " could not be instantiated.", e);
				}
			}

			for (final Class<? extends SchemaBuilder> c : lst)
			{
				if (Util.getName(c).equals(name))
				{
					try
					{
						return c.newInstance();
					}
					catch (InstantiationException | IllegalAccessException e)
					{
						throw new IllegalStateException(c
								+ " could not be instantiated.", e);
					}
				}
			}
			try
			{
				final Class<?> clazz = Class.forName(name);
				if (SchemaBuilder.class.isAssignableFrom(clazz))
				{
					try
					{
						return (SchemaBuilder) clazz.newInstance();
					}
					catch (InstantiationException | IllegalAccessException e)
					{
						throw new IllegalStateException(clazz
								+ " could not be instantiated.", e);
					}
				}
				else
				{
					throw new IllegalArgumentException(clazz
							+ " does not implement SchemaBuilder.");
				}
			}
			catch (final ClassNotFoundException e)
			{
				throw new IllegalArgumentException(e.getMessage());
			}
		}

		public static List<Class<? extends SchemaBuilder>> getBuilders()
		{
			final List<Class<? extends SchemaBuilder>> retval = new ArrayList<Class<? extends SchemaBuilder>>();

			final ClassLoaders loaders = ClassLoaders.getAppLoaders(
					SchemaBuilder.class, SchemaBuilder.class, false);
			final DiscoverClasses<SchemaBuilder> dc = new DiscoverClasses<SchemaBuilder>(
					loaders);

			final ResourceNameIterator classIter = (new DiscoverServiceNames(
					loaders)).findResourceNames(SchemaBuilder.class.getName());
			final List<String> lst = new ArrayList<String>();

			// we build a list first because the classes can be found by
			// multiple loaders.
			while (classIter.hasNext())
			{
				final String className = classIter.nextResourceName();
				if (!lst.contains(className))
				{
					lst.add(className);
				}
			}
			// now just load the classes once.
			for (final String className : lst)
			{
				final ResourceClassIterator<SchemaBuilder> iter = dc
						.findResourceClasses(className);
				while (iter.hasNext())
				{
					final Class<? extends SchemaBuilder> clazz = iter
							.nextResourceClass().loadClass();
					if (!retval.contains(clazz))
					{
						retval.add(clazz);
					}
				}
			}
			// return the list
			return retval;

		}

		public static String getDescription(
				final Class<? extends SchemaBuilder> clazz )
		{
			return Util.getField(clazz, "DESCRIPTION", clazz.getName());
		}

		private static String getField(
				final Class<? extends SchemaBuilder> clazz,
				final String fieldName, final String defaultValue )
		{
			try
			{
				final Field f = clazz.getField(fieldName);
				if (Modifier.isStatic(f.getModifiers()))
				{
					return f.get(null).toString();
				}
			}
			catch (final NoSuchFieldException e)
			{
				// do nothing -- acceptable
			}
			catch (final SecurityException e)
			{
				// do nothing -- acceptable
			}
			catch (final IllegalArgumentException e)
			{
				// do nothing -- acceptable
			}
			catch (final IllegalAccessException e)
			{
				// do nothing -- acceptable
			}
			return defaultValue;
		}

		public static String getName( final Class<? extends SchemaBuilder> clazz )
		{
			return Util.getField(clazz, "BUILDER_NAME", clazz.getSimpleName());
		}
	}

	Set<Table> getTableDefs( final RdfCatalog catalog );
}
