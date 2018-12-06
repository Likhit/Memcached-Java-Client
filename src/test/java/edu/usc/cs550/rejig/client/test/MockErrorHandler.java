package edu.usc.cs550.rejig.client.test;

import edu.usc.cs550.rejig.client.ErrorHandler;
import edu.usc.cs550.rejig.client.MemcachedClient;

class MockErrorHandler implements ErrorHandler {

	public void handleErrorOnInit(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnGet(
		final MemcachedClient client, final Throwable error,
		final String cacheKey) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnGet(
		final MemcachedClient client, final Throwable error,
		final String[] cacheKeys) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnSet(
		final MemcachedClient client, final Throwable error,
		final String cacheKey) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnDelete(
		final MemcachedClient client, final Throwable error,
		final String cacheKey) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnFlush(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnStats(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnConf(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnGrantLease(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnRevokeLease(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
	}

	public void handleErrorOnRefreshAndRetry(
		final MemcachedClient client, final Throwable error) {
			throw new RuntimeException(error);
		}
}