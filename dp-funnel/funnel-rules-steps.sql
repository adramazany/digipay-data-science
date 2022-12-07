select * from RULES;
select count(*) from swc_RULES;
select * from swc_RULES;
select * from swc_RULES_profiles;
select * from swc_RULES_steps;
select * from swc_RULES_steps_docs;

select * from swc_RULES
-- where id='6075c696f0319855a0d0b305' -- digipay
where id='62de8843c7a94e11d697c496' -- mellat
;
select * from swc_RULES_steps
-- where F_RULES='6075c696f0319855a0d0b305' -- digipay
where F_RULES='62de8843c7a94e11d697c496' -- mellat
order by id
;

-- truncate table swc_RULES_steps_docs;
-- truncate table swc_RULES_steps;
-- truncate table swc_RULES_profiles;
-- truncate table swc_RULES;

select * from DIM_FUNNEL_STEP;
select * from DIM_FUNNEL_MAPPING;
select * from DIM_FUNNEL_STATUS;

;

select no||'.'||row_number() over (partition by no order by cnt desc) as no,code,cnt from(
    select no,code,count(*) cnt from
    (select row_number() over (partition by f_rules order by id) as no,code from swc_RULES_steps)
    group by no,code order by no,count(*) desc)
;

select no,code,count(*) cnt from
    (select row_number() over (partition by f_rules order by id) as no,code from swc_RULES_steps)
group by no,code order by no,count(*) desc

;

select f_rules,row_number() over (partition by f_rules order by id) as no,code from swc_RULES_steps
;

select * from SWC_RULES_STEPS s
where s.CODE='BANK_SCORE_WITHOUT_PAY'
;

select F_RULES,row_number() over (partition by f_rules order by id) as no,code
from swc_RULES_steps
where F_RULES in (
    '62de8843c7a94e11d697c496',
    '62de8853c7a94e11d6980b93',
    '62e2d93fc7a94e11d6d8d8e8',
    '62f9ee93c7a94e11d6c7ea62',
    '62f9ef18c7a94e11d6c8a71f',
    '62f9ef5dc7a94e11d6c91393'
    );

create table swc_default_steps (
                                   no varchar(10) not null primary key,
                                   code varchar(100) not null unique
);
-- truncate table swc_default_steps;
insert into swc_default_steps values('01','REGISTER');
insert into swc_default_steps values('02','BANK_ACCOUNT_VERIFICATION');
insert into swc_default_steps values('03','DIGIPAY_SCORE');
insert into swc_default_steps values('04','FILING_PAYMENT');
insert into swc_default_steps values('05','PROFILE');
insert into swc_default_steps values('06','BANK_SCORE');
insert into swc_default_steps values('07','BANK_SCORE_WITHOUT_PAY');
insert into swc_default_steps values('08','UPLOAD');
insert into swc_default_steps values('09','OPENING_BANK_ACCOUNT');
insert into swc_default_steps values('10','CHEQUE_UPLOAD');
insert into swc_default_steps values('11','DIGITAL_SIGNATURE_AND_ONLINE_CONTRACT');
insert into swc_default_steps values('12','OFFLINE_CONTRACT');
insert into swc_default_steps values('13','ALLOCATION_PREPAYMENT');
insert into swc_default_steps values('14','WALLET_ACTIVATION');
select * from swc_default_steps
order by no
;


select F_RULES,row_number() over (partition by f_rules order by id) as no,code
,(select no from swc_default_steps ds where ds.CODE=s.code) as def_code
from swc_RULES_steps s
;

-- 5ec27f71195d8e40916872a2

select F_RULES,no,code,def_code,
    lead(def_code,1,0) over (partition by F_RULES order by no) as lead_def_code,
    case when lead(def_code,1,0) over (partition by F_RULES order by no)>def_code then 1 end as ex
from (
select F_RULES,row_number() over (partition by f_rules order by id) as no,code,
     (select no from swc_default_steps ds where ds.CODE=s.code) as def_code
from swc_RULES_steps s
) s2
;
select * from SWC_RULES where id='5ec27f71195d8e40916872a2';


create or replace view vw_swc_rules_steps_final as
with q_rules_steps_def_code as (
    select F_RULES,row_number() over (partition by f_rules order by id) as no,code,
           (select no from swc_default_steps ds where ds.CODE=s.code) as def_code
    from swc_RULES_steps s
--     where F_RULES='5ec27f71195d8e40916872a2'
    order by F_RULES,def_code)
,q_rules_steps_lag_code as (
    select t.*,lag(def_code,1) over(partition by f_rules order by no) lag_def_code
    from q_rules_steps_def_code t
--     union all (select 'ec27f71195d8e40916872ax' F_RULES,	3 NO,	'BANKACCOUNT' CODE,	'09' DEF_CODE,	'05' LAG_DEF_CODE from dual)
--     union all (select '5ec27f71195d8e40916872a1' F_RULES,	3 NO,	'BANK_SCORE' CODE,	'06' DEF_CODE,	'05' LAG_DEF_CODE from dual)
)
,q_rules_steps_ex_subcode as (
    select f_rules, no,code,def_code, lag_def_code||'.'||row_number() over (order by NO) final_code
        from q_rules_steps_lag_code
    where def_code<lag_def_code
)
select f_rules,no,code,def_code,
    (select final_code from q_rules_steps_ex_subcode ex2 where ex2.code=ex.code and ex2.def_code=ex.def_code and ROWNUM=1 ) as final_code
from q_rules_steps_ex_subcode ex
union all
select f_rules, no,code,def_code, def_code final_code
from q_rules_steps_lag_code
where lag_def_code is null or def_code>=lag_def_code
order by f_rules,final_code
;
-- drop table swc_rules_steps_final;
create table swc_rules_steps_final as select * from vw_swc_rules_steps_final;
create unique index ix_swc_rules_steps_final on swc_rules_steps_final(f_rules,code);

-- swc_default_steps
create or replace view vw_swc_steps_final as
select distinct code,def_code,final_code from vw_swc_rules_steps_final
;
--drop table swc_steps_final;
create table swc_steps_final as select * from vw_swc_steps_final order by final_code;

select count(*) from swc_rules_steps_final;--1101

select * from swc_rules_steps_final;
select * from swc_steps_final;
select * from swc_default_steps;


