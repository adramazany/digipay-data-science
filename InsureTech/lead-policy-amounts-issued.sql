select 'آنباندل'                                                                           "محصول",
       count(1)                                                                            "تعداد فروش",
       TO_CHAR(sum(pl1.product_price) / 10, 'fm999,999,999,999')                           "مبلغ کالا",
       TO_CHAR(sum(pl1.wage_amount) / 10, 'fm999,999,999,999')                             "کارمزد",
       TO_CHAR(sum(pl1.tax_amount) / 10, 'fm999,999,999,999')                              "مالیات",
       TO_CHAR(sum(pl1.discount_amount) / 10, 'fm999,999,999,999')                         "تخفیف",
       TO_CHAR(sum(pl1.total_amount) / 10, 'fm999,999,999,999')                            "کل مبلغ",
       TO_CHAR(sum(pl1.payable_amount) / 10, 'fm999,999,999,999')                          "دریافتی",
       TO_CHAR(sum(pl1.wage_amount * .26) / 10, 'fm999,999,999,999')                       "سهم دیجی‌کالا",
       0                                                                                   "سهم مرکز خسارت",
       TO_CHAR(sum(pl1.wage_amount * .44) / 10, 'fm999,999,999,999')                       "سهم شرکت بیمه",
       TO_CHAR(sum(pl1.payable_amount - (pl1.wage_amount * .7)) / 10, 'fm999,999,999,999') "سود فروش"
from unbundled.purchase p1
         join unbundled.basket b1 on b1.id = p1.basket_id
         join unbundled.lead l1 on l1.id = b1.lead_id
         join catalog.policy pl1 on pl1.sale_item_id = p1.id
where pl1.issued_at is not null
    /* FOR ALL TIME */
--   and true
    /* FROM BEGINNING OF Orderbehest till 7th Oribehesht */
--   and pl1.issued_at between '2022-05-20 19:30:00.000000 +00:00' and '2022-05-30 19:30:00.000000 +00:00'
    /* FROM BEGINNING OF 1401 */
  and pl1.issued_at > '2022-03-20 19:30:00.000000 +00:00'
    /* LAST 7 DAYS */
--   and pl1.issued_at
--     between CAST(CONCAT(CAST(CAST(now() - INTERVAL '8 day' as date) as varchar(10)), ' 19:30:00.000000 +00:00') as TIMESTAMP WITH TIME ZONE)
--     and CAST(CONCAT(CAST(CAST(now() - INTERVAL '1 day' as date) as varchar(10)), ' 19:30:00.000000 +00:00') as TIMESTAMP WITH TIME ZONE)
  and pl1.current_state between 300 and 511;





