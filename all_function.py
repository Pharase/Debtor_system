import pandas as pd
import sqlite3
from datetime import date

DB_PATH = r"C:\Pam_card\system_transform\database\Debtor_system.db"

priority_fields = ['Expense','Outlay','Loss','Penalty','Interest','Opportunity_cost','Charge','Principal','Replacement']

# === Function ===
def render_payment_table(df, columns_to_format):
    formatted_df = df.copy()

    for col in columns_to_format:
        if col in formatted_df.columns:
            def safe_format(x):
                try:
                    val = float(str(x).replace(",", "").strip())
                    return f"{val:,.2f}"
                except:
                    return x  # return original if it cannot be formatted

            formatted_df[col] = formatted_df[col].apply(safe_format)
        else:
            print(f"Warning: Column '{col}' not found in DataFrame.")

    return formatted_df.to_html(classes="table table-bordered", index=False, escape=False) if not formatted_df.empty else None

def calculate_accrued_interest_till_today(result_df, annual_rate=0.15):
    # Extract latest Paydate (not NaT, not empty)
    paydates = pd.to_datetime(result_df['Paydate'], errors='coerce')
    latest_date = paydates.dropna().max().date() if not paydates.dropna().empty else None
    today = date.today()

    if latest_date is None or latest_date >= today:
        return 0.0, 0  # No interest accrued or invalid range

    # Get final row for latest balances
    last_row = result_df.iloc[-1]
    principal = last_row.get("Principal", 0)
    replacement = last_row.get("Replacement", 0)
    base_amount = principal + replacement

    # Calculate interest
    days_passed = (today - latest_date).days
    daily_rate = annual_rate / 365
    accrued_interest = base_amount * daily_rate * days_passed

    return accrued_interest, days_passed
    
def get_initial_balance(pam_code):
    conn = sqlite3.connect(DB_PATH)
    #conn = get_mysql_connection()
    query = """
        SELECT 
            os.pam_code,
            os.transferred_expense_balance AS Expense,
            os.transferred_outlay_balance AS Outlay,
            os.transferred_loss_balance AS Loss,
            os.transferred_penalty_balance AS Penalty,
            os.transferred_oldinterest_balance_exc_vat AS Interest,
            os.transferred_opportunitycost_balance_exc_vat AS Opportunity_cost,
            os.transferred_oldcharge_balance_exc_vat AS Charge,
            os.transferred_principal_balance_exc_vat AS Principal,
            os.transferred_replacement_balance_exc_vat AS Replacement,
            os.transferred_all_balance_exc_vat AS 'หนี้รวมไม่แยกหมวดหมู่',
            os.outstanding_balance_excluding_vat AS ยอดหนี้ไม่รวมภาษี,
            os.vat_remain AS 'ภาษีคงเหลือ',
            os.outstanding_balance_including_vat AS ยอดหนี้รวมภาษี,

            name.old_contract_number,
            name.name,
            name.portfolio_credit_buying,
            name.transferred_product,
            name.transferred_status,
            name.transferred_mode,
            name.acquisition_date

        FROM Debtor_OS AS os
        LEFT JOIN Debtor_Name AS name
            ON os.pam_code = name.pam_code
        WHERE os.pam_code = ?
    """

    df = pd.read_sql_query(query, conn, params=(pam_code,))
    conn.close()

    # Convert part
    number_columns = [
    "Expense", "Outlay", "Loss", "Penalty", "Interest",
    "Opportunity_cost", "Charge", "Principal", "Replacement",
    "หนี้รวมไม่แยกหมวดหมู่", "ยอดหนี้ไม่รวมภาษี", "ภาษีคงเหลือ", "ยอดหนี้รวมภาษี"
    ]
    
    for col in number_columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .replace("-", None)
            .astype(float)
        )
        
    existing_cols = [col for col in number_columns if col in df.columns]
    df[existing_cols] = df[existing_cols].fillna(0)

    df["acquisition_date"] = pd.to_datetime(df["acquisition_date"], errors="coerce").dt.date
    return df.iloc[0] if not df.empty else None

def get_transactions(pam_code):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM Debtor_transaction WHERE pam_code = ?", conn, params=(pam_code,))
    df["TR_Date"] = df["TR_Date"].astype(str)
    df["Pay_Date"] = df["Pay_Date"].astype(str)
    df["EFF_Date"] = df["EFF_Date"].astype(str)
    conn.close()
    return df.sort_values(by="EFF_Date")  # Make sure payments are in order

def get_verdict(pam_code):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM Debtor_verdict WHERE pam_code = ?", conn, params=(pam_code,))
    conn.close()
    
    for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"] + ["ยอดหนี้ไม่รวมภาษี"] + ["ภาษีคงเหลือ"] + ["ยอดหนี้รวมภาษี"]:
        df[field] = df[field].astype(str).replace("-", "0")
        df[field] = df[field].replace(",", "", regex=True).astype(float)
        
    return df
        
# ----------------------------- Calculate ------------------------------------
def apply_formula(balance_row, transactions_df, verdict_df=None, int_function = False):
    
    interest_rate_annual = 0.15  # 15% per year
    daily_rate = interest_rate_annual / 365
    
    result = []
        
    # Prepare balance fields
    balance = (
        balance_row.get(priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"] + ["ยอดหนี้ไม่รวมภาษี"] + ["ภาษีคงเหลือ"] + ["ยอดหนี้รวมภาษี"], pd.Series(0))
        .replace("-", 0)
        .replace(",", "", regex=True)
        .fillna(0)
        .astype(float)
    )
    
    # Add initial acquisition snapshot
    acq_date = balance_row.get("acquisition_date", "")
    if isinstance(acq_date, str):
        try:
            acq_date = pd.to_datetime(acq_date).date()
        except:
            acq_date = ""
    initial_row = {
        "TRdate": "",
        "Paydate": "",
        "EFFdate": acq_date.strftime('%Y-%m-%d') if isinstance(acq_date, pd.Timestamp) else str(acq_date),
        "code": "aq",
        "mode": "",
        "total": "",
        "vat": "",
    }
    
    # Add all priority and summary fields as remaining balance
    for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่", "ภาษีคงเหลือ"]:
        initial_row[field] = balance[field]
        initial_row[f"{field}_remain"] = balance.get(field, 0)
    
    initial_row["ยอดหนี้ไม่รวมภาษี"] = balance["หนี้รวมไม่แยกหมวดหมู่"] if balance["หนี้รวมไม่แยกหมวดหมู่"] == 0 else sum(balance[field] for field in priority_fields)
    initial_row["ยอดหนี้รวมภาษี"] = initial_row["ยอดหนี้ไม่รวมภาษี"] + balance["ภาษีคงเหลือ"]
    
    result.append(initial_row)
    
    # Sort transactions by effective date before processing
    # Define your preferred code order
    custom_code_order = ["vd", "dis", "pm", "repo", "sold"]

    # Ensure code column follows this custom order
    transactions_df["code"] = pd.Categorical(
        transactions_df["code"],
        categories=custom_code_order,
        ordered=True
    )

    transactions_df["TR_Date"] = pd.to_datetime(transactions_df["TR_Date"], format="%d-%b-%y", errors="coerce")
    transactions_df["Pay_Date"] = pd.to_datetime(transactions_df["Pay_Date"], format="%d-%b-%y", errors="coerce")
    transactions_df["EFF_Date"] = pd.to_datetime(transactions_df["EFF_Date"], format="%d-%b-%y", errors="coerce")
    transactions_df = transactions_df.sort_values(by=["EFF_Date","code"])

    # Get VAT rate
    if balance.get("ภาษีคงเหลือ", 0) != 0:
        raw_vat_rate = balance.get("vat_rate", "7%")
        vat_rate = float(str(raw_vat_rate).replace('%', '').strip())
    else:
        vat_rate = 0
    
    last_eff_date = acq_date if acq_date else None
    
    for _, row in transactions_df.iterrows():
        code = row["code"]
        
        if (code == "pm") or (code == "sold") :
            payment = row['Amount']
        elif code == "dis":
            dis_amt = row['Amount']
        else:
            payment = 0
            
        Additional = row['Additional']
        Note = row['Note']
        mode = row['mode']
        try:
            payment = float(str(payment).replace(",", "").strip())
        except:
            payment = 0.0
            
        # Vat field select from proportion && Payment priority order
        if mode.endswith("_all_vat"):
            vat_field = ['หนี้รวมไม่แยกหมวดหมู่']
            
        elif mode.endswith("_vat"):
            if mode == "jm_vat":
                vat_field = ['Opportunity_cost','Charge','Replacement']
                
            elif mode == "cm_vat":
                vat_field = ['Opportunity_cost','Charge','Principal'] 
                
            elif mode == "npl_vat":
                vat_field = ['Interest','Principal'] 
        else:
            vat_field = []
        
        # Vat ratio calculate
        Debt_vat = 0
        total_exvat = 0
        replacement_amount = 0
        sold_amount_rec = 0
        for cal in vat_field:
            Debt_vat += balance[cal]
        if Debt_vat == 0:
            Debt_vat = balance["หนี้รวมไม่แยกหมวดหมู่"]
        for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"]:
            total_exvat += balance[field]
        vat_ratio = Debt_vat/total_exvat
        total_debt = sum(balance[field] for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"])  
        
        if payment >= total_debt:
            vat = result[-1]["ภาษีคงเหลือ"]
        else:
            vat = round(payment * vat_ratio * vat_rate / (100 + vat_rate), 2)
        
        net_payment_v = (payment * vat_ratio) - vat
        net_payment_nv = payment * (1 - vat_ratio)
        
        net_payment = net_payment_nv + net_payment_v
        
        allocation = {'TRdate': row['TR_Date'].date(), 'Paydate': row['Pay_Date'].date(), 'EFFdate': row['EFF_Date'].date(), "code": code, 'mode': mode, 'total': payment, 'vat': vat}
        
        # Interest part calculate
        if int_function == True :
            current_eff_date = row["Pay_Date"].date() if pd.notnull(row["Pay_Date"]) else None
        
            if last_eff_date and current_eff_date:
                days_passed = (current_eff_date - last_eff_date).days
                if days_passed > 0:
                    # Apply simple interest (e.g., 15% annually on Interest + Principal)
                    annual_rate = 0.15
                    daily_rate = annual_rate / 365
                    
                    for field in ["Principal","Replacement"]:  # or also "Principal", etc.
                        accrued = balance[field] * daily_rate * days_passed
                        balance["Interest"] += accrued
            
                    # Optionally store interest as a row in the result
                    interest_row = {
                        "TRdate": row["TR_Date"].date(),
                        "Paydate": current_eff_date,
                        "EFFdate": row["EFF_Date"].date(),
                        "code": "int",
                        "mode": "addition",
                        "total": 0,
                        "vat": 0,
                    }
                    for field in priority_fields:
                        interest_row[field] = balance[field]
                    interest_row["ภาษีคงเหลือ"] = balance["ภาษีคงเหลือ"]
                    interest_row["ยอดหนี้ไม่รวมภาษี"] = sum(balance.get(f, 0) for f in priority_fields)
                    interest_row["ยอดหนี้รวมภาษี"] = interest_row["ยอดหนี้ไม่รวมภาษี"] + interest_row["ภาษีคงเหลือ"]
                    result.append(interest_row)
            
            # Update for next iteration
            last_eff_date = current_eff_date
        
        propotion_part = []
        
        for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"]:
            if balance[field] > 0:
                propotion_part.append(field)
        
        available_vat = []        
        
        for field in vat_field:
            if balance[field] > 0:
                available_vat.append(field)
        
        # Find which fields are both in propotion_part and available_vat
        vat_related_proportion = [field for field in propotion_part if field in vat_field]
        non_vat_related_proportion = [field for field in propotion_part if field not in vat_field]

        if code == "repo":
            if mode == "jm_vat":
                for field in propotion_part:
                    if field == "Replacement" and balance[field] > 0:
                        replacement_amount = balance[field]
                        balance[field] = 0
                        
            else:
                for field in propotion_part:
                    allocation[field] = balance[field]
                
        elif code == "vd":
            if verdict_df is not None:
                partial_row = verdict_df[verdict_df["pam_code"] == balance_row["pam_code"]].iloc[0]
                  
                # Optionally store interest as a row in the result
                for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่", "ภาษีคงเหลือ"]:
                    if field == "Expense":
                        balance[field] = partial_row[field] + Additional
                    balance[field] = partial_row[field]  

        elif code == "sold":
            # Cut portion part
            if Note == "Auction":
                vat = (payment * vat_rate/(100 + vat_rate))
                net_payment = payment - vat 
                
            else:
                net_payment = payment * (100/(100 + vat_rate))
                vat = payment - net_payment
                
            sold_amount_rec = net_payment
            
            if mode == "jm_vat":
                if balance["Replacement"] > 0:
                    to_cut = min(balance["Replacement"], net_payment)
                    balance["Replacement"] -= to_cut
                    allocation["Replacement"] = to_cut
                    net_payment -= to_cut
            else:
                for field in ['Interest', 'Principal']:
                    if balance[field] > 0:
                        if field == 'Principal':
                            # No remaining left
                            to_cut = net_payment
                        else:
                            # Last field gets the remainder to avoid rounding loss
                            proportion = balance[field] / (balance["Interest"] + balance["Principal"])
                            to_cut = net_payment * proportion

                        actual_cut = min(balance[field], to_cut)
                        balance[field] -= actual_cut
                        allocation[field] = actual_cut
                        net_payment -= actual_cut
                    
            net_payment_v = (payment * vat_ratio) - vat
            net_payment_nv = payment * (1 - vat_ratio)   
            
            for field in propotion_part:
                if field == "Expense":
                    balance[field] = balance[field] + Additional
                        
                if mode == "jm_vat":
                    if net_payment <= 0 or field == "Replacement": 
                        continue
                elif mode == "npl_vat":
                    if net_payment <= 0 or field == "Interest" or field == "Principal":
                        continue
                    
                reducer = 0
                
                if field in vat_related_proportion:
                    idx = vat_related_proportion.index(field)
                    reducer = sum(balance[f] for f in vat_related_proportion[:idx + 1])
                    part = net_payment_v
                elif field in non_vat_related_proportion:
                    idx = non_vat_related_proportion.index(field)
                    reducer = sum(balance[f] for f in non_vat_related_proportion[:idx + 1])
                    part = net_payment_nv                  

                if mode.endswith("_all_vat"):
                    to_cut = reducer
                else:
                    to_cut = part if (part - reducer) < 0 else (part - reducer)
                actual_cut = min(balance[field], to_cut, part)

                balance[field] -= actual_cut
                allocation[field] = actual_cut
                net_payment -= actual_cut
                if field in vat_related_proportion:
                    net_payment_v -= actual_cut
                else:
                    net_payment_nv -= actual_cut
            
            allocation = {'TRdate': row['TR_Date'].date(), 'Paydate': row['Pay_Date'].date(), 'EFFdate': row['EFF_Date'].date(), "code": code, 'mode': mode, 'total': payment, 'vat': vat}
                
        elif code == "dis":
        
            # mark payment as pending for now; apply after N times matched
            # dis_amt = 10581.33
            if dis_amt < 1:
                discount = dis_amt 
            else:
                discount = dis_amt/balance["ยอดหนี้รวมภาษี"]
            
            vat = 0    
            allocation["total"] = f"{discount*100:.2f} %"
            allocation["vat"] = vat
            for field in propotion_part + ["ภาษีคงเหลือ"]:
                balance[field] = balance[field] * (1-discount)
            # Store temporarily if implementing later match-check logic

        else:  
            # Cut part logic
            if net_payment >= total_debt:
                # Pay off everything directly — no need for proportion
                for field in propotion_part:
                    actual_cut = balance[field]
                    balance[field] = 0
                    allocation[field] = actual_cut
                net_payment -= total_debt
                
            else:
                
                total_nv = 0
                total_v = 0

                if Additional != 0:
                    balance["Expense"] += Additional

                i_v = 0
                i_nv = 0

                while net_payment > 0:
                    total_v = sum(balance[f] for f in vat_related_proportion)
                    total_nv = sum(balance[f] for f in non_vat_related_proportion)

                    # Transfer remaining payment if one side is fully paid
                    if net_payment_nv > 0 and total_nv == 0:
                        net_payment_v += net_payment_nv
                        net_payment_nv = 0
                    if net_payment_v > 0 and total_v == 0:
                        net_payment_nv += net_payment_v
                        net_payment_v = 0

                    # Cut VAT-related part
                    if i_v < len(vat_related_proportion) and net_payment_v > 0:
                        field = vat_related_proportion[i_v]
                        is_last = (i_v == len(vat_related_proportion) - 1)
                        to_cut = net_payment_v if is_last else round(net_payment_v * 1, 2)  # Use actual proportion if needed

                        actual_cut = min(balance[field], to_cut)
                        balance[field] -= actual_cut
                        allocation[field] = allocation.get(field, 0) + actual_cut
                        net_payment_v -= actual_cut
                        net_payment -= actual_cut
                        i_v += 1  # Only increase if field was processed

                    # Cut non-VAT-related part
                    if i_nv < len(non_vat_related_proportion) and net_payment_nv > 0:
                        field = non_vat_related_proportion[i_nv]
                        is_last = (i_nv == len(non_vat_related_proportion) - 1)
                        to_cut = net_payment_nv if is_last else round(net_payment_nv * 1, 2)  # Use actual proportion if needed

                        actual_cut = min(balance[field], to_cut)
                        balance[field] -= actual_cut
                        allocation[field] = allocation.get(field, 0) + actual_cut
                        net_payment_nv -= actual_cut
                        net_payment -= actual_cut
                        i_nv += 1  # Only increase if field was processed

                    # If both parts are depleted, break
                    if net_payment_v <= 0 and net_payment_nv <= 0:
                        break

               
        for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่"]:   
            allocation[field] = balance[field]
        
        balance["ภาษีคงเหลือ"] -= vat
        if balance["ภาษีคงเหลือ"] >= 0 :
            allocation["ภาษีคงเหลือ"] = balance["ภาษีคงเหลือ"] 
        else:
            allocation["ภาษีคงเหลือ"] = 0
        result.append(allocation)
  
    # Handle discount logic post-loop
    #result = handle_discount_logic(result, dis_groups, balance)
        
    # Append remaining balances
    remaining_dict = balance.to_dict()
    remaining_dict["TRdate"] = "Remaining Balance"
    remaining_dict["Paydate"] = ""
    remaining_dict["EFFdate"] = ""
    remaining_dict["code"] = ""
    remaining_dict["mode"] = ""
    remaining_dict["total"] = ""
    remaining_dict["vat"] = ""
    
    for field in priority_fields + ["หนี้รวมไม่แยกหมวดหมู่","ภาษีคงเหลือ"]:    
        remaining_dict[field] = max(0, remaining_dict.get(field, 0))
        
    if replacement_amount > 0:
        remaining_dict["Replacement"] = sold_amount_rec - replacement_amount    
    
    result.append(remaining_dict)

    df_result = pd.DataFrame(result).fillna(0)

    # Calculate ยอดหนี้ไม่รวมภาษี
    component_fields = ['Expense', 'Outlay', 'Loss', 'Penalty', 'Interest',
                        'Opportunity_cost', 'Charge', 'Principal', 'Replacement']

    df_result["ยอดหนี้ไม่รวมภาษี"] = df_result.apply(
        lambda row: row["หนี้รวมไม่แยกหมวดหมู่"] if row.get("หนี้รวมไม่แยกหมวดหมู่", 0) != 0
        else sum(row.get(field, 0) for field in component_fields),
        axis=1
    )

    # Calculate ยอดหนี้รวมภาษี
    df_result["ยอดหนี้รวมภาษี"] = df_result["ยอดหนี้ไม่รวมภาษี"] + df_result["ภาษีคงเหลือ"]

    # Define column order
    final_columns = ['TRdate', 'Paydate', 'EFFdate', 'code', 'mode', 'total', 'vat'] + priority_fields + ["หนี้รวมไม่แยกหมวดหมู่","ยอดหนี้ไม่รวมภาษี","ภาษีคงเหลือ","ยอดหนี้รวมภาษี"]

    # Reorder and return
    return df_result[final_columns]