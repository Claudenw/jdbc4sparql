package org.xenei.jdbc4sparql;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LoggingConfig
{
	public static ConsoleAppender getConsole()
	{
		if (LoggingConfig.console == null)
		{
			LoggingConfig.console = new ConsoleAppender(); // create appender
			// configure the appender
			final String PATTERN = "%d [%p|%c|%C{1}] %m%n";
			LoggingConfig.console.setLayout(new PatternLayout(PATTERN));
			LoggingConfig.console.setThreshold(Level.INFO);
			LoggingConfig.console.activateOptions();
			// add appender to any Logger (here is root)
			Logger.getRootLogger().addAppender(LoggingConfig.console);
		}
		return LoggingConfig.console;
	}

	public static FileAppender getLog()
	{
		if (LoggingConfig.log == null)
		{
			LoggingConfig.log = new FileAppender();
			LoggingConfig.log.setName("FileLogger");
			LoggingConfig.log.setFile("/tmp/loggingConfig.log");
			LoggingConfig.log.setLayout(new PatternLayout(
					"%d %-5p [%c{1}] %m%n"));
			LoggingConfig.log.setThreshold(Level.DEBUG);
			LoggingConfig.log.setAppend(true);
			LoggingConfig.log.activateOptions();
			Logger.getRootLogger().addAppender(LoggingConfig.log);
		}
		return LoggingConfig.log;
	}

	public static void setConsole( final Level level )
	{
		LoggingConfig.getConsole().setThreshold(level);
	}

	public static void setLog( final Level level )
	{
		LoggingConfig.getLog().setThreshold(level);
	}

	public static void setLogger( final Class<?> clazz, final Level level )
	{
		Logger.getLogger(clazz).setLevel(level);
	}

	public static void setLogger( final String name, final Level level )
	{
		Logger.getLogger(name).setLevel(level);
	}

	public static void setRootLogger( final Level level )
	{
		Logger.getRootLogger().setLevel(level);
	}

	private static ConsoleAppender console;

	private static FileAppender log;
}
