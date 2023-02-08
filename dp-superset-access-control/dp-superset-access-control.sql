-- drop role superapp;
create role dp_common_role;
create role dp_user_role;
create role dp_credit_c_role;
create role dp_credit_funnel_role;
create role dp_credit_bnpl_role;
create role dp_credit_card_role;
create role dp_credit_merchant_role;
create role dp_credit_score_role;
create role dp_superapp_role;
create role dp_payment_role;
create role dp_insuretech_role;
create role dp_kyc_role;
create role dp_finance_role;
create role dp_fpa_role;
create role dp_gl_role;
create role dp_marketing_role;
create role dp_okr_role;
create role dp_operation_role;
create role dp_test_role;

--------- dp_credit_bnpl_role
grant select on MV_BNPL_ACTIVATION to dp_credit_bnpl_role;


set serveroutput on size unlimited

declare
    v_role varchar2(100) := 'dp_credit_bnpl_role';
    v_tables varchar2(4000) := 'MONGODB.MV_BNPL_ACTIVATION,
MONGODB.MV_BNPL_FIRST_PURCHASE,
MONGODB.MV_BNPL_FIX_PURCHASE_ONE_YEAR_AFTER,
MONGODB.MV_BNPL_MERCHANT_GROUP,
MONGODB.MV_BNPL_MERCHANT_PURCHASE,
MONGODB.MV_BNPL_PURCHASES,
MONGODB.MV_BNPL_PURCHASES_BASED_JUST_BUSINESS,
MONGODB.MV_BUSINESSES_BNPL,
MONGODB.MV_CREDIT_BNPL_ALLOCATION,
MONGODB.MV_CREDIT_BNPL_INSTALLMENT,
MONGODB.MV_CREDIT_BNPL_PAID_INSTALLMENT';
    v_sql varchar2(4000);
begin
    for t in (select regexp_substr(v_tables,'[^,]+', 1, level) table_name from dual
                connect BY regexp_substr(v_tables, '[^,]+', 1, level) is not null ) loop
        v_sql:='grant select on '||t.table_name||' to '||v_role;
        dbms_output.put_line(v_sql );
        execute immediate v_sql;
    end loop;
end;
/

-- select regexp_substr('A1,A2,A4','[^,]+', 1, level) from dual
-- connect BY regexp_substr('A1,A2,A4', '[^,]+', 1, level) is not null
