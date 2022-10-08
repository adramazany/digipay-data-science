import pandas as pd
from io import BytesIO
import requests
import gspread

# 1 => not worked
# sheet_id = "1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1"
# sheet_name = "Sheet1"
# url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

# 2 => worked with : view anyone with the link
# sheet_url = "https://docs.google.com/spreadsheets/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/edit#gid=0"
# url = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

# # 3 not worked
# url = 'https://docs.google.com/spreadsheets/d/' + \
# '1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1' + \
# '/export?gid=0&format=csv'

# 4 not worked
# url = 'https://docs.google.com/spreadsheet/ccc?key=1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1&output=csv'
# url = "https://docs.google.com/spreadsheets/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/export?format=csv&gid="
# r = requests.get(url)
# data = r.content
# print(data)

# 5 not worked
# import gspread
# credentials = {
#     "installed": {
#         "client_id": "12345678901234567890abcdefghijklmn.apps.googleusercontent.com",
#         "project_id": "my-project1234",
#         "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         ...
#     }
# }
# authorized_user = {
#     "refresh_token": "8//ThisALONGTOkEn....",
#     "token_uri": "https://oauth2.googleapis.com/token",
#     "client_id": "12345678901234567890abcdefghijklmn.apps.googleusercontent.com",
#     "client_secret": "MySecRet....",
#     "scopes": [
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/drive"
#     ],
#     "expiry": "1070-01-01T00:00:00.000001Z"
# }
# gc, authorized_user = gspread.oauth_from_dict(credentials, authorized_user)
#
# sh = gc.open("Example spreadsheet")
#
# print(sh.sheet1.get('A1'))

# 6
url = "https://docs.google.com/spreadsheets/d/1BwD2OvPzWPk4fsJeJdNhKvyDysicKmDKo4nSR8yE1-8/export?format=csv&gid=0"

print(url)
# 1-3,6
df =  pd.read_csv(url,header=0)
# 4
# df = pd.read_csv(BytesIO(data), index_col=0)

print(df)
