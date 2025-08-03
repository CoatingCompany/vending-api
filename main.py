from flask import Flask, request, jsonify
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json

app = Flask(__name__)

# Load credentials from file
with open("service_account.json") as f:
    creds_dict = json.load(f)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# Open spreadsheet
SHEET_NAME = "Wonder Toys - База Данни"
sheet = client.open(SHEET_NAME).sheet1

@app.route("/")
def index():
    return "Vending API is running on Google Cloud!"

@app.route("/get", methods=["POST"])
def get_latest():
    data = request.json
    location = data.get("location", "").strip().lower()
    all_records = sheet.get_all_records()
    filtered = [r for r in all_records if r["Локация"].strip().lower() == location]
    if not filtered:
        return jsonify({"message": f"Няма запис за локация: {location}"}), 404
    latest = sorted(filtered, key=lambda r: datetime.strptime(r["Последно зареждане"], "%B %d, %Y"), reverse=True)[0]
    return jsonify(latest)

@app.route("/add", methods=["POST"])
def add_record():
    data = request.json
    location = data.get("location")
    consumables = data.get("consumables")
    date = data.get("date", datetime.now().strftime("%B %d, %Y"))
    notes = data.get("notes", "")
    sheet.append_row([location, consumables, date, notes])
    return jsonify({"message": f"Записът за {location} беше добавен успешно."})

@app.route("/delete", methods=["POST"])
def delete_record():
    data = request.json
    location = data.get("location", "").strip().lower()
    date = data.get("date", None)
    all_records = sheet.get_all_records()
    for i, row in enumerate(all_records):
        if row["Локация"].strip().lower() == location and (not date or row["Последно зареждане"] == date):
            sheet.delete_rows(i + 2)
            return jsonify({"message": f"Изтрит запис за {location} от {row['Последно зареждане']}"})
    return jsonify({"message": "Не е намерен такъв запис."}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
