# 🧾 Debtor System – Flask Web App for Debt Balance Checking

A simple and functional web application built with **Python Flask** to check the outstanding balances of debtors.  
This system allows internal staff to search for debtor information and view current balance details in real-time.


<img width="1852" height="883" alt="image" src="https://github.com/user-attachments/assets/894de83c-7f75-425f-99b2-bac27a50772d" />


---

## 🚀 Features

- 🔍 **Search for debtors** by ID
- 💰 **Display remaining balances**, separated by components (principal, interest, penalty, etc.)
- 🧾 **View payment history** 
- 📊 Summary of debtor payments in systems
- 🛡️ Accessible via browser, no installation required

---

## 🛠 Tech Stack

| Component        | Description                |
|------------------|----------------------------|
| **Backend**      | Python 3.13 + Flask        |
| **Frontend**     | HTML, Bootstrap    |
| **Database**     | SQLite or CSV-based        |
| **Deployment**   | Flask built-in dev server  |

---

## 🧪 How to Run

```bash
pip install flask flask_sqlalchemy pandas openpyxl
cp .env.example .env      # set DEBTOR_DB_PATH, TRANSACTION_FOLDER and (if needed) LEGACY_COLUMN_PREFIX
python update_transaction_to_db.py   # import new transactions from the latest summary_data_file_*.xlsx
python app.py                        # open http://127.0.0.1:5000
```

On Windows, `Run_Debtor_engine.bat` does both. If your database or Excel exports still use an older column
prefix, set `LEGACY_COLUMN_PREFIX`: Excel headers are renamed on load, and database columns are renamed once
(a `.before-rename.bak` copy of the database is saved first).
