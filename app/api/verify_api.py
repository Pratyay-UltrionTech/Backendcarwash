import requests

# Try to fetch the report data directly from the backend
url = "http://localhost:8000/api/v1/admin/reports/washers?mobile=true&period=month"
headers = {
    "Authorization": "Bearer YOUR_TOKEN_HERE" # I don't have a token
}

# Actually, I can't do this easily without a token.
# I'll just check the DB again using a different method to run the script.
