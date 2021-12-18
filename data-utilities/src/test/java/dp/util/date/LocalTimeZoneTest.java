package dp.util.date;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class LocalTimeZoneTest {

//    {'creationDate': [1542456210000.0],
//        'datetime': ['2018-11-17T15:33:14.407Z'],
//        'datetime2': ['2018-11-17T12:03:14.407Z'],
//        'oid': ['6761296061542455976810']}}],

    @Test
    public void testLocalTimeZone(){
        System.out.println("now="+new Date());                 //Tue Dec 08 11:56:12 IRST 2020
        System.out.println("2018-11-17T15:33:14.407Z timestamp2date="+new Date(1542456210000L));   //Sat Nov 17 15:33:30 IRST 2018
        System.out.println("2018-11-17T15:33:14.407Z timestamp2date.GMT="+new Date(1542456210000L).toGMTString());   //Sat Nov 17 15:33:30 IRST 2018
        System.out.println("2018-11-17T15:33:14.407Z timestamp2date.Local="+new Date(1542456210000L).toLocaleString());   //Sat Nov 17 15:33:30 IRST 2018
        System.out.println("new Date().getTimezoneOffset() = " + new Date().getTimezoneOffset());

        Timestamp ts = new Timestamp(1542456210000L);
        System.out.println("2018-11-17T15:33:14.407Z timestamp="+ts.toLocalDateTime());
        System.out.println("2018-11-17T15:33:14.407Z timestamp="+new Date(ts.getTime()));

        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.setTimeInMillis(1542456210000L);
        System.out.println("2018-11-17T15:33:14.407Z timestamp2utc_calendar="+c.getTime());

        System.out.println(java.util.Arrays.toString(TimeZone.getAvailableIDs()));
        //ZONEMAPPINGS.add(new TimeZoneMapping("Iran Standard Time", "Asia/Tehran", "(GMT +03:30) Tehran"));
        Calendar c2 = Calendar.getInstance(TimeZone.getTimeZone("UTC+03:30"));
        c2.setTimeInMillis(1542456210000L);
        System.out.println("2018-11-17T15:33:14.407Z timestamp2+03:30_calendar="+c2.getTime());

        Calendar c3 = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tehran"));
        c3.setTimeInMillis(1542456210000L);
        System.out.println("2018-11-17T15:33:14.407Z timestamp2+Asia/Tehran_calendar="+c3.getTime());

        System.out.println(new Date());

    }
}
