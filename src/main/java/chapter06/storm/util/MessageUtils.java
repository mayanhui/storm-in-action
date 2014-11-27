package chapter06.storm.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import kafka.message.Message;

public class MessageUtils {
	public static final String DEFAULT_ENCODING = "utf8";

	public static String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		try {
			return new String(bytes, DEFAULT_ENCODING);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
}
