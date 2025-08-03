from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

app = Flask(__name__)

# Load service account key
with open("service_account.json") as f:
    creds_data = json.load(f)

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_data, scope)
client = gspread.authorize(creds)

# Replace with your actual Google Sheet name
SHEET_NAME = "Wonder Toys - База Данни"
sheet = client.open(SHEET_NAME).sheet1

@app.route("/")
def home():
    return "Vending API is running."

@app.route("/get", methods=["GET"])
def get_latest_by_location():
    location = request.args.get("location")
    data = sheet.get_all_records()
    filtered = [row for row in data if row["Място"] == location]
    if not filtered:
        return jsonify({"message": "Няма намерени записи."})
    return jsonify(filtered[-1])

@app.route("/add", methods=["POST"])
def add_entry():
    entry = request.get_json()
    values = [entry.get("Място", ""), entry.get("Дата", ""), entry.get("Консумативи", ""), entry.get("Бележки", "")]
    sheet.append_row(values)
    return jsonify({"message": "Добавен запис успешно."})

@app.route("/delete", methods=["POST"])
def delete_last_by_location():
    location = request.json.get("Място")
    all_values = sheet.get_all_values()
    header = all_values[0]
    body = all_values[1:]
    for i in range(len(body) - 1, -1, -1):
        if body[i][0] == location:
            sheet.delete_row(i + 2)
            return jsonify({"message": f"Изтрит последен запис за {location}."})
    return jsonify({"message": "Няма запис за изтриване."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
