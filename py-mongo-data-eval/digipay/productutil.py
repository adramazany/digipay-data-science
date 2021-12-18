
def fill_app_ipg_wallet_products(rows):
    app_types=[12, 15, 16, 17, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 140, 170];
    topup_types=[30,31];
    bill_types=[40];
    ipg_gateways=[0,1];
    ipg_types=[0, 1, 16, 30, 31, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];
    wallet_types=[15, 16, 17];
    wallet_gateway4_types=[0, 1, 30, 31, 32, 40, 70, 80, 92, 100, 110, 111, 112, 113, 130, 140, 150, 160, 170];
    for row in rows:
        if row["type"] in app_types : row["is_app"] = 1
        if row["type"] in topup_types : row["is_topup"] = 1
        if row["type"] in bill_types : row["is_bill"] = 1
        if row["type"] in ipg_types \
                and row["gateway"] in ipg_gateways \
                and 'owner_debtor_cellNumber' in row \
                and row['owner_debtor_cellNumber'] :
            row["is_ipg"] = 1
        if (row["type"] in wallet_types) \
                or( row["gateway"] ==4 \
                and row["type"] in wallet_gateway4_types) :
            row["is_wallet"] = 1


