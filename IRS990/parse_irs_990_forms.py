import requests

index = "https://s3.amazonaws.com/irs-form-990/index_2018.json"

a = requests.get(index)
print(a.status_code)
