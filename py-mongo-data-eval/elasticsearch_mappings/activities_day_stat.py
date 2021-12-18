import requests

body = {
    "properties": {
      "amount": {
        "type": "long"
      },
      "count": {
        "type": "long"
      },
      "date_time": {
        "type": "date"
      },
      "feeCharge": {
        "type": "long"
      },
      "jalali_date_time": {
        "type": "text"
      },
      "jalali_day": {
        "type": "long"
      },
      "jalali_month": {
        "type": "long"
      },
      "jalali_year": {
        "type": "long"
      },
      "oid": {
        "type": "date",
        "format": "epoch_millis"
      }
    }
  }

resp = requests.put('http://localhost:9200/activities_day_stat/_mapping',json=body)
if resp.status_code != 200:
  # This means something went wrong.
  raise Exception('GET /tasks/ {}'.format(resp.status_code))
print("content=",resp.content)

