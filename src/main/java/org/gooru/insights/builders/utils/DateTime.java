package org.gooru.insights.builders.utils;

import java.util.concurrent.TimeUnit;

public class DateTime {

	public static String convertMillisecondsToDate(long millis) {
		String time = String.format("%02d:%02d:%02d:%03d", TimeUnit.MILLISECONDS.toHours(millis),
			    TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
			    TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1),
			    millis % TimeUnit.SECONDS.toMillis(1));
		return time;
	}
	
	public static String convertMillisecondsToTime(long millis) {
		String time = String.format("%02d hr %02d min %02d sec", TimeUnit.MILLISECONDS.toHours(millis),
			    TimeUnit.MILLISECONDS.toMinutes(millis) % TimeUnit.HOURS.toMinutes(1),
			    TimeUnit.MILLISECONDS.toSeconds(millis) % TimeUnit.MINUTES.toSeconds(1));
		return time;
	}
}
