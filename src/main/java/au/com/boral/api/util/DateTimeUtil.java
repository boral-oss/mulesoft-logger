package au.com.boral.api.util;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {

	private static final String LOCAL_ZONE = "Australia/Sydney";
	private static final String UTC_ZONE = "UTC";
	
	private static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private static final String SIMPLE_DATE_TIME_FORMAT = "yyyy-MM-dd' 'HH:mm:ss.SSS";
	private static final String ISO_OFFSET_DATE_TIME_WITH_ZEROS = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
	
	public static String currentISODateTime() {
		return ZonedDateTime.now(ZoneId.of(LOCAL_ZONE)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	}
	
	public static String currentISODateTimeWithZeros() {
		return ZonedDateTime.now(ZoneId.of(LOCAL_ZONE)).format(DateTimeFormatter.ofPattern(ISO_OFFSET_DATE_TIME_WITH_ZEROS));
	}
	
	public static String toString(Date date) {
		SimpleDateFormat format = new SimpleDateFormat(ISO_8601_DATE_FORMAT);
		return format.format(date);
	}
	
	public static String currentSimpleDateTime() {
		return ZonedDateTime.now(ZoneId.of(LOCAL_ZONE)).format(DateTimeFormatter.ofPattern(SIMPLE_DATE_TIME_FORMAT));
	}
	
	public static String currentUTCDateTime() {
		return ZonedDateTime.now(ZoneId.of(UTC_ZONE)).format(DateTimeFormatter.ofPattern(SIMPLE_DATE_TIME_FORMAT));
	}
}
