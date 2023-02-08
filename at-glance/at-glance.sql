-- BNPL
select to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian') YM
     ,to_char(sum(c.credit_amount),'999,999,999,999') credit_amount
from MONGODB.contract c
where c.fundprovidercode='DIGIPAY'
--     and c.end_gdate between to_date('14010101','YYYYMMDD','nls_calendar=persian')
--     and to_date('14010131','YYYYMMDD','nls_calendar=persian')
group by to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian')
order by to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian')
;

-- C-CREDIT
select to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian') YM
     ,to_char(sum(c.credit_amount),'999,999,999,999') credit_amount
from MONGODB.contract c
where c.fundprovidercode not in ('DIGIPAY')
--     and c.end_gdate between to_date('14010101','YYYYMMDD','nls_calendar=persian')
--     and to_date('14010131','YYYYMMDD','nls_calendar=persian')
group by to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian')
order by to_char(c.end_gdate,'YYYY/MM','nls_calendar=persian') desc
;


-- Retention Rate
select
        a.APP_2AND1_MONTHS / a.APP_2_MONTHS as "APP Retention Rate"
        ,to_char(a.GDATE ,'yyyymm','nls_calendar=persian') as "ماه سال"
from mongodb.FT_OKR_APP_CUSTOMER_RETENTION a
where to_char(a.gdate + 1,'dd','nls_calendar=persian') = '01'
order by GDATE desc
;

-- M-CREDIT
-- where STATUS in (5,6,7,8)
;





INSERT INTO DP_AT_A_GLANCE.STG_AT_GLANCE
select GDATE,PDATE,KPI,KPI_DETAILS,MEASURE
from FINANCE.vw_fin_at_glance d
where not exists(select * from DP_AT_A_GLANCE.STG_AT_GLANCE sg
                 where sg.kpi=d.kpi and sg.PDATE=d.pdate)
;

select * from DP_AT_A_GLANCE.STG_AT_GLANCE
    where kpi in (9) and pdate=14010231;

select * from DP_AT_A_GLANCE.DIM_AT_GLANCE_KPI;
select distinct kpi from DP_AT_A_GLANCE.FT_AT_GLANCE;

select count(*) from DP_AT_A_GLANCE.STG_AT_GLANCE where kpi in (9,10,11,12,17);
select * from DP_AT_A_GLANCE.STG_AT_GLANCE where kpi in (9,10,11,12,17);
-- delete from DP_AT_A_GLANCE.STG_AT_GLANCE where kpi in (9,10,11,12,17);
select * from DP_AT_A_GLANCE.STG_AT_GLANCE
-- delete from DP_AT_A_GLANCE.STG_AT_GLANCE
where kpi in (13,14,15,16,17) and pdate=14010131
;


-- update dp_at_a_glance.ft_at_glance s
-- set s.yoy_measure = (select ss.measure from dp_at_a_glance.ft_at_glance ss where ss.kpi = s.kpi and s.yoy_date = ss.pdate)
-- where s.pdate >=13970101 ;
--
-- update dp_at_a_glance.ft_at_glance s
-- set s.mom_measure = (select ss.measure from dp_at_a_glance.ft_at_glance ss where ss.kpi = s.kpi and s.mom_date = ss.pdate)
-- where s.pdate >= 13970101;


begin
    DP_AT_A_GLANCE.P_AT_GLANCE_DAILY;
end;

-- begin
--     DP_AT_A_GLANCE.P_AT_GLANCE;
-- end;


select * from DP_AT_A_GLANCE.FT_AT_GLANCE
-- where kpi in (9,10,11,12,17) and pdate=14010231
where kpi in (16) and pdate=14010131
;
