package io.hzmsg;

public interface Consumer {
	void start();
	
	void shutdown();
	
	void consume(final Message message, final HZDataListener hzDataListener);
	
	boolean retry(final Message message);
}