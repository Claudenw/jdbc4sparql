package org.xenei.jdbc4sparql.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

/**
 * A zip input stream that does not close when "close" is called. Instead it
 * calls closeEntry. To close the stream call close on the original input
 * stream.
 */
public class NoCloseZipInputStream extends InputStream {
	ZipInputStream wrapped;

	public NoCloseZipInputStream(final ZipInputStream is) {
		wrapped = is;
	}

	@Override
	public int available() throws IOException {
		return wrapped.available();
	}

	/**
	 * A close implementation that calls closeEntry() instead. To really close
	 * the stream call close on the inputstream that was used in the
	 * constructor.
	 */
	@Override
	public void close() throws IOException {
		wrapped.closeEntry();
	}

	@Override
	public boolean equals(final Object obj) {
		return wrapped.equals(obj);
	}

	@Override
	public int hashCode() {
		return wrapped.hashCode();
	}

	@Override
	public void mark(final int readlimit) {
		wrapped.mark(readlimit);
	}

	@Override
	public boolean markSupported() {
		return wrapped.markSupported();
	}

	@Override
	public int read() throws IOException {
		return wrapped.read();
	}

	@Override
	public int read(final byte[] b) throws IOException {
		return wrapped.read(b);
	}

	@Override
	public int read(final byte[] b, final int off, final int len)
			throws IOException {
		return wrapped.read(b, off, len);
	}

	@Override
	public void reset() throws IOException {
		wrapped.reset();
	}

	@Override
	public long skip(final long n) throws IOException {
		return wrapped.skip(n);
	}

	@Override
	public String toString() {
		return wrapped.toString();
	}

}