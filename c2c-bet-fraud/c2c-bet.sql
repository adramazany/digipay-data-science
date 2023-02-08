-- select * from PC_PAYMENTS p
select * from PAY_PRECOURSE_PAYMENTS p
where /*STATUS=8
  and */type='CARD_TRANSFER'
and DESTINATION_PREFIX='502908'
and DESTINATION_POSTFIX='6409'
and JD_CREATIONDATE=14001201
-- and to_char(GD_CREATIONDATE,'YYYY/MM/DD HH24','nls_calendar=persian')='1400/12/01 18'
-- and to_char(GD_CREATIONDATE,'YYYY/MM/DD HH24:MI','nls_calendar=persian')='1400/12/01 18:32'
;

select * from DIM_PAYMENTS_STATUS;
-- کارت مقصد	نوع تراکنش	تاریخ	زمان	کد مرجع	مبلغ	کد پذیرنده	شماره ترمینال	نام شرکت پرداخت ساز
-- 5029081059836409	کارت به کارت	1400/12/01	18:32:20	32528042319	0	6	125	نوآوران پرداخت مجازي ايرانيان

select max(GD_CREATIONDATE) from PAY_PRECOURSE_PAYMENTS;
-- 2022-04-18 19:59:58.000000

select TRACKINGCODE,amount,status,INITIATOR,ip,OWNER_CELLNUMBER,SOURCE_PREFIX,SOURCE_POSTFIX
     ,SOURCE_BANK_NAME,DESTINATION_BANK_NAME,DEBTOR_CELLNUMBER,CREDITOR_NAME
     ,to_char(GD_CREATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') CREATIONDATE
     ,to_char(GD_EXPIRATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') EXPIRATIONDATE
from PAY_PRECOURSE_PAYMENTS p
where type='CARD_TRANSFER'
  and DESTINATION_PREFIX='502908'
  and DESTINATION_POSTFIX='6409'
  and GD_CREATIONDATE between to_date('1400/12/01 18:32:20','YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')-(1/86400*10)
                          and to_date('1400/12/01 18:32:20','YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')+(1/86400*10)
;


select to_date('1400/12/01 18:32:20','YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')-(1/86400*10) from dual;


select b.PAN_DEST,substr(b.PAN_DEST,1,6) prefix,substr(b.PAN_DEST,13,4) postfix
     ,b."DATE"||' '||substr(b.TIME,1,8)
     ,b.*
from c2c_bet b
where "DATE">='1400/10/01'
;

select *
from c2c_bet b
where "DATE">='1400/10/01'
and not exists (
select TRACKINGCODE,amount,status,INITIATOR,ip,OWNER_CELLNUMBER,SOURCE_PREFIX,SOURCE_POSTFIX
     ,SOURCE_BANK_NAME,DESTINATION_BANK_NAME,DEBTOR_CELLNUMBER,CREDITOR_NAME
     ,to_char(GD_CREATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') CREATIONDATE
     ,to_char(GD_EXPIRATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') EXPIRATIONDATE
from PAY_PRECOURSE_PAYMENTS p
where type='CARD_TRANSFER'
  and DESTINATION_PREFIX=substr(b.PAN_DEST,1,6)
  and DESTINATION_POSTFIX=substr(b.PAN_DEST,13,4)
  and GD_CREATIONDATE between to_date(b."DATE"||' '||substr(b.TIME,1,8),'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')-(1/86400*10)
    and to_date(b."DATE"||' '||substr(b.TIME,1,8),'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')+(1/86400*10)
)
;

select b.PAN_DEST "کارت مقصد"
     ,b.TRANSACTIONTYPE "نوع تراکنش"
     ,b."DATE" "تاریخ"
     ,b.TIME "زمان"
     ,b.RRN "کد مرجع"
     ,b.AMOUNT "مبلغ"
     ,b.MERCHANTNO "کد پذیرنده"
     ,b.TERMINALNO "شماره ترمینال"
     ,b.CHANNEL "نام شرکت پرداخت ساز"
     ,case p.status when '8' then 'SUCCEED' when '6' then 'EXPIRED' end status
     ,p.OWNER_CELLNUMBER "شماره همراه مورد استفاده برای ثبتنام و فعالسازی برنامک"
     ,p.ip as "آدرس اینترنتی کاربر ایجادکننده تراکنش"
     ,p.INITIATOR as "نوع برنامک مورد استفاده برای انجام تراکنش(Native/PWA)"
     ,to_char(u.ACTIVATION_GDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') "تاریخ ثبتنام و فعالسازی برنامک مورداستفاده برای انجام تراکنش"
     ,p.TRACKINGCODE "مشخصه منحصربفرد برنامک انجام دهنده تراکنش"
     ,to_char(p.GD_CREATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') "زمان دقیق دریافت درخواست انتقال وجه"
     ,null "زمان دقیق دریافت درخواست کنترل اطلاعات کارت مقصد"
     ,null "نسخه برنامک مورداستفاده برای انجام تراکنش"
     ,null "سایر مشخصات دریافت شده از محیط اجرای برنامک انجام دهنده تراکنش"
     ,p.amount,p.SOURCE_PREFIX,p.SOURCE_POSTFIX
    ,p.SOURCE_BANK_NAME,p.DESTINATION_BANK_NAME,p.DEBTOR_CELLNUMBER,p.CREDITOR_NAME
    ,to_char(p.GD_EXPIRATIONDATE,'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian') EXPIRATIONDATE
from c2c_bet b
left join PAY_PRECOURSE_PAYMENTS p on (p.type='CARD_TRANSFER'
          and p.DESTINATION_PREFIX=substr(b.PAN_DEST,1,6)
          and p.DESTINATION_POSTFIX=substr(b.PAN_DEST,13,4)
          and p.GD_CREATIONDATE between to_date(b."DATE"||' '||substr(b.TIME,1,8),'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')-(1/86400*10)
          and to_date(b."DATE"||' '||substr(b.TIME,1,8),'YYYY/MM/DD HH24:MI:SS','nls_calendar=persian')+(1/86400*10)
          )
left join UAA_USERS u on u.USER_ID=p.OWNER_USERID
where "DATE">='1400/10/01'
order by rrn
;

select * from UAA_USERS
;


آدرس اینترنتی کاربر ایجادکننده تراکنش
نوع برنامک مورد استفاده برای انجام تراکنش(Native/PWA)

زمان دقیق دریافت درخواست کنترل اطلاعات کارت مقصد
نسخه برنامک مورداستفاده برای انجام تراکنش
سایر مشخصات دریافت شده از محیط اجرای برنامک انجام دهنده تراکنش





