package dp.util.date;

import dp.util.NVL;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.*;

public class JalaliCalendarTest {


    @Test
    public void test() {

        JalaliCalendar jc = new JalaliCalendar(new Date());
        System.out.println("jc = " + jc.toString());

        JalaliCalendar jc2 = new JalaliCalendar(1399,8,27);
        System.out.println("jc2.toGregorian().getTime() = " + jc2.toGregorian().getTime());
        System.out.println("jc2.toGregorian().getTime().ms = " + jc2.toGregorian().getTime().getTime());



        //System.out.println(PersianDateUtil.jalaliToDate("1399/08/27"));

    }

    @Test
    public void startEnd(){
        int start_year  =1398 ,start_month  =10  ,start_day  =1;
        int end_year    =1399   ,end_month  =9    ,end_day  =30;

        JalaliCalendar jc = new JalaliCalendar(start_year,start_month,start_day);
        System.out.println(String.format("START => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , jc.toGregorian().getTime() , jc.toGregorian().getTime().getTime() ));

        jc = new JalaliCalendar(end_year,end_month,end_day);
        GregorianCalendar gc = jc.getTomorrow().toGregorian();
        gc.add(Calendar.MILLISECOND,-1);
        System.out.println(String.format("END => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , gc.getTime() , gc.getTime().getTime() ));

    }

    @Test
    public void startEndMonth(){
        int year=1399;
        int month=1;
        //for(year=1399;year>=1397;year--)
            for(month=1;month<=12;month++)
        {
                JalaliCalendar jc = new JalaliCalendar(year, month, 1);
                //System.out.println(String.format("START => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , jc.toGregorian().getTime() , jc.toGregorian().getTime().getTime() ));
                System.out.println(String.format("%s\t%s\t%d\t%s", jc.toString(), jc.toGregorian().getTime(), jc.toGregorian().getTime().getTime(), "day start"));

                int monthLastDay = jc.getMonthLength();
                jc.setDay(monthLastDay);
                GregorianCalendar gc = jc.getTomorrow().toGregorian();
                gc.add(Calendar.MILLISECOND, -1);
                //System.out.println(String.format("END => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , gc.getTime() , gc.getTime().getTime() ));
                System.out.println(String.format("%s\t%s\t%d\t%s", jc.toString(), gc.getTime(), gc.getTime().getTime(), "day end"));
            }
    }

    @Test
    public void startEndQuarter(){
        int year=1399;
        int q=4;
        int month=1;
        switch (q){
            case 2:month=4;break;
            case 3:month=7;break;
            case 4:month=10;break;
        }
        JalaliCalendar jc = new JalaliCalendar(year,month,1);
        System.out.println(String.format("START => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , jc.toGregorian().getTime() , jc.toGregorian().getTime().getTime() ));

        jc.setMonth(month+2);
        int monthLastDay = jc.getMonthLength();
        jc.setDay(monthLastDay);
        GregorianCalendar gc = jc.getTomorrow().toGregorian();
        gc.add(Calendar.MILLISECOND,-1);
        System.out.println(String.format("END => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , gc.getTime() , gc.getTime().getTime() ));

    }

    @Test
    public void startEndH1(){
        int year=1399;
        JalaliCalendar jc = new JalaliCalendar(year,1,1);
        System.out.println(String.format("START => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , jc.toGregorian().getTime() , jc.toGregorian().getTime().getTime() ));

        jc.setMonth(6);
        int monthLastDay = jc.getMonthLength();
        jc.setDay(monthLastDay);
        GregorianCalendar gc = jc.getTomorrow().toGregorian();
        gc.add(Calendar.MILLISECOND,-1);
        System.out.println(String.format("END => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , gc.getTime() , gc.getTime().getTime() ));

    }

    @Test
    public void startDates(){
        String[] dates={
          "1399/01/01"
          ,"1399/11/01"
          ,"1399/12/01"
          ,"1400/01/01"
        };
        for(String d : dates){
            String[] ar=d.split("/");
            int start_year  = NVL.getInt(ar[0]) ,start_month  =NVL.getInt(ar[1])  ,start_day  =NVL.getInt(ar[2]);
            JalaliCalendar jc = new JalaliCalendar(start_year,start_month,start_day);
            System.out.println(String.format("START => jalalDate = %s , gregorianDate = %s , ms = %d" , jc.toString() , jc.toGregorian().getTime() , jc.toGregorian().getTime().getTime() ));

        }

    }

}