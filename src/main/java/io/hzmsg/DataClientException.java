package io.hzmsg;

public class DataClientException extends RuntimeException {
	public DataClientException() {
		
	}
	
	public DataClientException(String message) {
		super(message);
	}
	
	public DataClientException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public DataClientException(Throwable cause) {
		super(cause);
	}
}
