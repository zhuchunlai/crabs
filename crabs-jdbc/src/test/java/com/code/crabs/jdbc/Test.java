package com.code.crabs.jdbc;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author zhuchunlai
 * @version $Id: Test.java, v1.0 2014/09/03 15:15 $
 */
public final class Test {

    public static void main(final String[] args) throws ParseException {
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String dateString = "1999-02-28 00:00:00";
        final Date date = dateFormat.parse(dateString);
        System.out.println(date.getTime());
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        final TimeZone src = calendar.getTimeZone();
        final TimeZone target = TimeZone.getTimeZone("UTC");
        System.out.println(calendar.getTimeZone());
        System.out.println(TimeZone.getTimeZone("UTC"));
        System.out.println(date.getTime() + src.getRawOffset() - target.getRawOffset());
    }

}
