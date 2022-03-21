import pyspark.sql.functions as F

LOYALTY_CARDS = "hive_ssa_tc5.loyalty_card"
LOYALTY_CARDHOLDERS = "hive_ssa_tc5.loyalty_cardholder"
ACCOUNTS = "hive_ssa_tc5.account"
CVM5_GUESTS = "hive_cvm_acrm.cvm5_guests"

DIM_STORE = "hive_ssa_main.dim_store"
CHECKS_HEADERS = "hive_ssa_main.fct_rtl_txn"
CHECKS_ITEMS = "hive_ssa_main.fct_rtl_txn_item"
PRODUCTS = "hive_ssa_tc5.cvm_product"


def get_segment_grouped_one_plu(spark, start_date, end_date, plu_codes, guests, type_data='standart'):
    """Returns grouped plu_code, plu_nm, plu_qty, optional rto for some guest_segment.
    
    :param start_date, end_date: campaign start/end date 
    :type start_date, end_date: datetime.date
    :param plu_codes: list of plu codes for grouped
    :type plu_codes: list of int
    :param guests: segment of guests(only one column - customer_rk)
    :type guests: spark.df
    :param type_data: can be 'standart', 'with_rto'. 'standart' - plu_code, plu_name, plu_qty. 'with_rto' - the same + rto for each plu.
    :type type_data: str
    
    :rtype: pandas dataframe
    :return: info about plu - plu_code, plu_nm, plu_qty, rto(optional)
    """
    checks_headers = (spark.table(CHECKS_HEADERS)
                      .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                      .filter((F.col('loyalty_card_no') != '') & (F.col('loyalty_card_no').isNotNull()))
                      .filter(F.col('financial_unit_format_dk') == 'D')
                      .filter(F.col('rtl_txn_cancel_flg') == 0)
                      .select('rtl_txn_id', 'loyalty_card_no')
                     )
    
    loyalty_cards = (spark
                    .table(LOYALTY_CARDS)
                    .withColumnRenamed("loyalty_card_id", "loyalty_card_no")
                    .withColumnRenamed("loyalty_account_id", "account_no")
                    .withColumnRenamed("loyalty_account_acrm_id", "account_rk")
                    .select('account_no', 'loyalty_card_no')
                )
    loyalty_cardholders = (spark
                            .table(LOYALTY_CARDHOLDERS)
                            .filter(F.col('loyalty_cardholder_age_yrs') >= 18)
                            .withColumnRenamed("loyalty_cardholder_acrm_id", "customer_rk")
                            .withColumnRenamed("loyalty_account_id", "account_no")
                            .select('account_no', 'customer_rk')
                          )
    clients_info = loyalty_cards.join(loyalty_cardholders, on='account_no', how='inner')
    clients_info = clients_info.join(guests, on='customer_rk', how='inner')
    
    checks_headers_tc5 = checks_headers.join(clients_info, on='loyalty_card_no') #оставили чеки только доступных гостей
    
    checks_items = (spark.table(CHECKS_ITEMS) 
                    .withColumnRenamed('plu_id', 'plu_code')
                    .withColumnRenamed('turnover_no_vat_amt', 'zsalnovat')
                    .withColumnRenamed('turnover_vat_rub_amt', 'zsale_vat')
                    .withColumnRenamed('prime_cost_no_vat_amt', 'zcst_n')
                    .withColumnRenamed('turnover_base_uom_amt', 'base_qty')
                    .withColumnRenamed('discount_amt', 'zdiscount')
                    .withColumnRenamed('fact_regular_promo_flg', 'zpromofl')
                    .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                    .filter((F.col('zsalnovat') >= 0) & (F.col('base_qty') >= 0) & (F.col('zcst_n') > 0))
                        .select('plu_code' #items id
                                , 'rtl_txn_id' #cheques id
                                , 'base_qty'
                                , 'zsalnovat'
                                , 'zsale_vat'
                               )
                   )
    checks_items = checks_items.filter(F.col('plu_code').isin(plu_codes)) # только чеки с нужными plu
    checks_tc5 = checks_items.join(checks_headers_tc5, 'rtl_txn_id', how='inner')
    if type_data == 'with_rto':
        grouped = (checks_tc5.groupby(['plu_code']).agg(F.sum('base_qty').alias('plu_qty'),
                                                  F.sum('zsalnovat').alias('rto_novat'),
                                                  F.sum('zsale_vat').alias('rto_vat')))
    else:
        grouped = checks_tc5.groupby(['plu_code']).agg(F.sum('base_qty').alias('plu_qty'))
    plu_names = (spark.table(PRODUCTS)
                      .withColumnRenamed('plu_id', 'plu_code')
                      .select('plu_code', 'plu_nm'))
    seg = grouped.join(plu_names, 'plu_code', 'inner')
    seg_pd = seg.toPandas()
    if len(seg_pd.columns) > 3:
        seg_pd = seg_pd[['plu_code', 'plu_nm', 'plu_qty', 'rto_novat', 'rto_vat']]
    else:
        seg_pd = seg_pd[['plu_code', 'plu_nm', 'plu_qty']]
    return seg_pd


def get_segment_grouped_several_plu(spark, start_date, end_date, plu_codes, guests):
    checks_headers = (spark.table(CHECKS_HEADERS)
                      .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                      .filter((F.col('loyalty_card_no') != '') & (F.col('loyalty_card_no').isNotNull()))
                      .filter(F.col('financial_unit_format_dk') == 'D')
                      .filter(F.col('rtl_txn_cancel_flg') == 0)
                      .select('rtl_txn_id', 'loyalty_card_no')
                     )
    
    loyalty_cards = (spark
                    .table(LOYALTY_CARDS)
                    .withColumnRenamed("loyalty_card_id", "loyalty_card_no")
                    .withColumnRenamed("loyalty_account_id", "account_no")
                    .withColumnRenamed("loyalty_account_acrm_id", "account_rk")
                    .select('account_no', 'loyalty_card_no')
                )
    loyalty_cardholders = (spark
                            .table(LOYALTY_CARDHOLDERS)
                            .filter(F.col('loyalty_cardholder_age_yrs') >= 18)
                            .withColumnRenamed("loyalty_cardholder_acrm_id", "customer_rk")
                            .withColumnRenamed("loyalty_account_id", "account_no")
                            .select('account_no', 'customer_rk')
                          )
    clients_info = loyalty_cards.join(loyalty_cardholders, on='account_no', how='inner')
    clients_info = clients_info.join(guests, on='customer_rk', how='inner')
    
    checks_headers_tc5 = checks_headers.join(clients_info, on='loyalty_card_no') #оставили чеки только доступных гостей
    
    checks_items = (spark.table(CHECKS_ITEMS) 
                    .withColumnRenamed('plu_id', 'plu_code')
                    .withColumnRenamed('turnover_no_vat_amt', 'zsalnovat')
                    .withColumnRenamed('turnover_vat_rub_amt', 'zsale_vat')
                    .withColumnRenamed('prime_cost_no_vat_amt', 'zcst_n')
                    .withColumnRenamed('turnover_base_uom_amt', 'base_qty')
                    .withColumnRenamed('discount_amt', 'zdiscount')
                    .withColumnRenamed('fact_regular_promo_flg', 'zpromofl')
                    .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                    .filter((F.col('zsalnovat') >= 0) & (F.col('base_qty') >= 0) & (F.col('zcst_n') > 0)) #keep only correct data
                        .select('plu_code' #items id
                                , 'rtl_txn_id' #cheques id
                                , 'base_qty'
                               )
                   )
    checks_items = checks_items.filter(F.col('plu_code').isin(plu_codes)) # только чеки с нужными plu
    checks_tc5 = checks_items.join(checks_headers_tc5, 'rtl_txn_id', how='inner')
    columns = ['rtl_txn_id']
    pdf = checks_tc5.groupby(columns).agg(F.sum('base_qty').alias('base_qty_per_check'))
    pdf2 = (pdf.withColumn('label', F.when((F.col('base_qty_per_check') <= 5), "label1")
                                 .when((F.col('base_qty_per_check').isin([6, 7])), "label2")
                                 .when((F.col('base_qty_per_check').isin([8, 9])), "label3")
                                 .when((F.col('base_qty_per_check').isin([10, 11])), "label4")
                                 .when((F.col('base_qty_per_check').isin([12, 13, 14, 15])), "label5")
                                 .when((F.col('base_qty_per_check').isin([16, 17, 18, 19, 20])), "label6")
                                 .when((F.col('base_qty_per_check') >= 21), "label7"))
       )
    pdf3 = pdf2.groupby(['label']).agg(F.countDistinct('rtl_txn_id').alias('qty_checks'))
    seg_pd = pdf3.toPandas()
    return seg_pd


def get_param_general(spark, start_date, end_date, plu_codes, guests):
    checks_headers = (spark.table(CHECKS_HEADERS)
                      .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                      .filter((F.col('loyalty_card_no') != '') & (F.col('loyalty_card_no').isNotNull()))
                      .filter(F.col('financial_unit_format_dk') == 'D')
                      .filter(F.col('rtl_txn_cancel_flg') == 0)
                      .select('rtl_txn_id', 'loyalty_card_no')
                     )
    
    loyalty_cards = (spark
                    .table(LOYALTY_CARDS)
                    .withColumnRenamed("loyalty_card_id", "loyalty_card_no")
                    .withColumnRenamed("loyalty_account_id", "account_no")
                    .withColumnRenamed("loyalty_account_acrm_id", "account_rk")
                    .select('account_no', 'loyalty_card_no')
                )
    loyalty_cardholders = (spark
                            .table(LOYALTY_CARDHOLDERS)
                            .filter(F.col('loyalty_cardholder_age_yrs') >= 18)
                            .withColumnRenamed("loyalty_cardholder_acrm_id", "customer_rk")
                            .withColumnRenamed("loyalty_account_id", "account_no")
                            .select('account_no', 'customer_rk')
                          )
    clients_info = loyalty_cards.join(loyalty_cardholders, on='account_no', how='inner')
    clients_info = clients_info.join(guests, on='customer_rk', how='inner')
    
    checks_headers_tc5 = checks_headers.join(clients_info, on='loyalty_card_no') #оставили чеки только доступных гостей
    
    checks_items = (spark.table(CHECKS_ITEMS) 
                    .withColumnRenamed('plu_id', 'plu_code')
                    .withColumnRenamed('turnover_no_vat_amt', 'zsalnovat')
                    .withColumnRenamed('turnover_vat_rub_amt', 'zsale_vat')
                    .withColumnRenamed('prime_cost_no_vat_amt', 'zcst_n')
                    .withColumnRenamed('turnover_base_uom_amt', 'base_qty')
                    .withColumnRenamed('discount_amt', 'zdiscount')
                    .withColumnRenamed('fact_regular_promo_flg', 'zpromofl')
                    .filter(F.col('rtl_txn_dt').between(start_date, end_date))
                    .filter((F.col('zsalnovat') >= 0) & (F.col('base_qty') >= 0) & (F.col('zcst_n') > 0)) #keep only correct data
                        .select('plu_code' #items id
                                , 'rtl_txn_id' #cheques id
                                , 'base_qty'
                                , 'zsale_vat'
                                , 'zsalnovat'
                               )
                   )
    checks_items = checks_items.filter(F.col('plu_code').isin(plu_codes)) # только чеки с нужными plu
    checks_tc5 = checks_items.join(checks_headers_tc5, 'rtl_txn_id', how='inner')
    grouped = checks_tc5.agg(F.countDistinct('account_no').alias('guest_qty'),
                            F.sum('base_qty').alias('product_qty'),
                            F.sum('zsale_vat').alias('rto_vat'),
                            F.sum('zsalnovat').alias('rto_novat'))
    
    grouped_pd = grouped.toPandas()
    return grouped_pd