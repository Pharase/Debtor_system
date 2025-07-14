from flask import Flask, request, render_template, send_file, redirect, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy # type: ignore
import sqlite3
import pandas as pd
import os
from datetime import date, datetime
from all_function import render_payment_table, calculate_accrued_interest_till_today,get_initial_balance,get_transactions,get_verdict,apply_formula

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "mysql+pymysql://datateamtestuser:DataTeamTestPassword%402024@18.140.56.138:3306/Debtor_system"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

#with app.app_context():
#    DB_PATH = db.engine.raw_connection()
    
DB_PATH = r"C:\Pam_card\system_transform\database\Debtor_system.db"

# === Configuration ===
UPLOAD_FOLDER = 'uploads/'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Payment priority order
debt_order = [
    "transferred_expense_balance",
    "transferred_outlay_balance",
    "transferred_loss_balance",
    "transferred_penalty_balance",
    "transferred_oldinterest_balance_exc_vat",
    "transferred_opportunitycost_balance_exc_vat",
    "transferred_oldcharge_balance_exc_vat",
    "transferred_principal_balance_exc_vat",
    "transferred_replacement_balance_exc_vat"
]

priority_fields = ['Expense','Outlay','Loss','Penalty','Interest','Opportunity_cost','Charge','Principal','Replacement']

def get_mysql_connection():
    return db.engine.raw_connection()
    
# === Routes ===
@app.route('/')
def index():
    return redirect(url_for('home'))

@app.route('/home')
def home():
    try:
        conn = sqlite3.connect(DB_PATH)
        #conn = get_mysql_connection()
        df = pd.read_sql_query("SELECT * FROM Debtor_OS", conn)
        for col in df.columns:
            if col != "pam_code":
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).round(4)
                
        # Summary queries
        cursor = conn.cursor()
        cursor.execute("""
            SELECT SUM(CAST(REPLACE(REPLACE(outstanding_balance_including_vat, ',', ''), '฿', '') AS REAL)) 
            FROM Debtor_OS
        """)
        total_os_amount = cursor.fetchone()[0]

        # Monthly payment summary
        collect_df = pd.read_sql_query("""
                SELECT Amount, Pay_Date FROM Debtor_transaction WHERE code = 'pm' AND Note = 0 """, conn)
        sold_receive_auction_df = pd.read_sql_query("""
                SELECT Amount, Pay_Date FROM Debtor_transaction WHERE code != 'aq' AND code != 'dis' AND Note = 'Auction'""", conn)
        sold_receive_direct_df = pd.read_sql_query("""
                SELECT Amount, Pay_Date FROM Debtor_transaction WHERE code != 'aq' AND code != 'dis' AND Note = 'Direct'""", conn)
        conn.close()
        
        # Ensure date parsing
        collect_df['Pay_Date'] = pd.to_datetime(collect_df['Pay_Date'], format="%d-%b-%y", errors='coerce')
        collect_df = collect_df.dropna(subset=['Pay_Date'])
        collect_df = collect_df.sort_values(by='Pay_Date')
        collect_df['YearMonth'] = collect_df['Pay_Date'].dt.to_period('M').dt.to_timestamp()
        monthly_summary = collect_df.groupby('YearMonth')['Amount'].sum().sort_index()

        sold_receive_auction_df['Pay_Date'] = pd.to_datetime(sold_receive_auction_df['Pay_Date'], format="%d-%b-%y", errors='coerce')
        sold_receive_auction_df = sold_receive_auction_df.dropna(subset=['Pay_Date'])
        sold_receive_auction_df = sold_receive_auction_df.sort_values(by='Pay_Date')
        sold_receive_auction_df['YearMonth'] = sold_receive_auction_df['Pay_Date'].dt.to_period('M').dt.to_timestamp()
        auction_summary = sold_receive_auction_df.groupby('YearMonth')['Amount'].sum().sort_index()

        sold_receive_direct_df['Pay_Date'] = pd.to_datetime(sold_receive_direct_df['Pay_Date'], format="%d-%b-%y", errors='coerce')
        sold_receive_direct_df = sold_receive_direct_df.dropna(subset=['Pay_Date'])
        sold_receive_direct_df = sold_receive_direct_df.sort_values(by='Pay_Date')
        sold_receive_direct_df['YearMonth'] = sold_receive_direct_df['Pay_Date'].dt.to_period('M').dt.to_timestamp()
        direct_summary = sold_receive_direct_df.groupby('YearMonth')['Amount'].sum().sort_index()

        # To pass to your frontend:
        #months = monthly_summary.index.strftime('%Y-%b').tolist()
        
        all_months = sorted(set(monthly_summary.index) | set(auction_summary.index) | set(direct_summary.index))

        # add
        months = [d.strftime('%Y-%b') for d in all_months]       

        monthly_summary = monthly_summary.reindex(all_months, fill_value=0)
        auction_summary = auction_summary.reindex(all_months, fill_value=0)
        direct_summary = direct_summary.reindex(all_months, fill_value=0)

        amounts = monthly_summary.tolist()
        auction_amounts = auction_summary.tolist()
        direct_amounts = direct_summary.tolist() 
        
        collect_payment = sum(collect_df['Amount'])
        sold_receive_auction = sum(sold_receive_auction_df['Amount'])
        sold_receive_direct = sum(sold_receive_direct_df['Amount'])
        
        row_count = len(df)
        now = datetime.now().strftime('%A, %d %B %Y %H:%M')

        return render_template(
            'home.html',
            time=now,
            rows=row_count,
            total_OS_temp=total_os_amount,
            total_payment_rec=collect_payment,
            total_auc_rec=sold_receive_auction,
            total_dir_rec=sold_receive_direct,
            months=months,
            amounts=amounts,
            auction_amounts=auction_amounts,
            direct_amounts=direct_amounts
        )
    except Exception as e:
        return f"Error loading home page: {e}"

@app.route('/debtors', methods=['GET', 'POST'])
def debtors():
    head_info = pd.DataFrame()
    summary_df = pd.DataFrame()
    payment_table_html = None
    accrued_interest = None
    accrued_days = None
    total_os = None
    int_engine = False  
    
    if request.method == 'POST':
        debtor_id = request.form.get('debtor_id')
        int_engine = request.form.get('int_engine') == 'on'
        try:
            conn = sqlite3.connect(DB_PATH)
            
            # Head info
            head_query = "SELECT * FROM Debtor_Name WHERE pam_code = ?"
            head_df  = pd.read_sql_query(head_query, conn, params=(debtor_id,))
            head_info = head_df[["pam_code","old_contract_number","name","portfolio_credit_buying","transferred_product",
                            "transferred_status","transferred_mode","acquisition_date","int_rate_info"
                            ]]
            head_info = head_info.rename(columns={
                                    "old_contract_number": "เลขที่สัญญาเดิม",
                                    "name": "ชื่อ - สกุล",
                                    "portfolio_credit_buying": "Port",
                                    "acquisition_date" : "วันที่ซื้อพอร์ต",
                                    "int_rate_info" : "อัตราดอกเบี้ยเดิม"
                                })
            
            # Transaction history
            initial = get_initial_balance(debtor_id)
            if initial is not None:
                transactions = get_transactions(debtor_id)
                verdict = get_verdict(debtor_id)
                summary_df = apply_formula(initial, transactions,verdict_df=verdict,int_function = int_engine)
                # Extract the last TotalOS (ยอดหนี้รวมภาษี)
                try:
                    total_os = summary_df["ยอดหนี้รวมภาษี"].iloc[-1]
                except KeyError:
                    total_os = summary_df["TotalOS"].iloc[-1]
                    
                if int_engine == True :
                    accrued_interest, days = calculate_accrued_interest_till_today(summary_df)
                    accrued_days = days
                    if accrued_interest > 0:
                        interest_row = summary_df.iloc[[-1]].copy()
                        interest_row["TRdate"] = "Total with Accrued Interest"
                        interest_row["code"] = ""
                        interest_row["Interest"] += accrued_interest
                        interest_row["ยอดหนี้ไม่รวมภาษี"] += accrued_interest
                        interest_row["ยอดหนี้รวมภาษี"] += accrued_interest
                        summary_df = pd.concat([summary_df, interest_row], ignore_index=True)
            conn.close()
        except Exception as e:
            return f"Error querying database: {e}"
               
    head_table_html = head_info.to_html(classes='table table-striped', index=False) if not head_info.empty else None
    
    columns_to_display = ["total", "vat"] + priority_fields + ["หนี้รวมไม่แยกหมวดหมู่", "ยอดหนี้ไม่รวมภาษี", "ภาษีคงเหลือ", "ยอดหนี้รวมภาษี"]
    payment_table_html = render_payment_table(summary_df, columns_to_display)
    
    return render_template(
        "debtors.html",
        head_table_html=head_table_html,
        payment_table_html=payment_table_html,
        total_os=total_os,
        accrued_interest=accrued_interest,
        accrued_days=accrued_days
    )

@app.route('/accounting', methods=['GET', 'POST'])
def accounting():
    
    return render_template(
        "accounting.html",

    )

@app.route('/search_pamcode')
def search_pamcode():
    query = request.args.get('q', '').strip()

    if not query or len(query) < 2:
        return jsonify([])

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Match PAM codes that start with the query
    cursor.execute("SELECT pam_code FROM Debtor_OS WHERE pam_code LIKE ? LIMIT 10", (query + '%',))
    matches = [row[0] for row in cursor.fetchall()]

    conn.close()
    return jsonify(matches)    
    
@app.route('/management', methods=['GET', 'POST'])
def management():
    message = ""
    if request.method == 'POST':
        file = request.files['file']
        if file and file.filename.endswith('.xlsx'):
            filepath = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(filepath)

            try:
                df = pd.read_excel(filepath)
                conn = sqlite3.connect(DB_PATH)
                df.to_sql("debtors", conn, if_exists="append", index=False)
                conn.close()
                message = f"✅ Uploaded and saved {file.filename}"
            except Exception as e:
                message = f"❌ Error processing file: {e}"
        else:
            message = "⚠️ Only .xlsx files are supported"

    return render_template('management.html', message=message)

@app.route('/download_report')
def download_report():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM debtors", conn)
        conn.close()

        report_path = os.path.join(UPLOAD_FOLDER, 'summary_report.xlsx')
        df.to_excel(report_path, index=False)
        return send_file(report_path, as_attachment=True)
    except Exception as e:
        return f"❌ Error generating report: {e}"

# === Run the App ===
if __name__ == '__main__':
    app.run(debug=True)
