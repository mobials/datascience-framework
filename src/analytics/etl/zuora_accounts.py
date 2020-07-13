import http.client

conn = http.client.HTTPSConnection("https://rest.zuora.com/")

payload = "{\n    \"compression\": \"NONE\", \n    \"output\": {\n        \"target\": \"S3\"\n    }, \n    \"outputFormat\": \"JSON\", \n    \"query\": \"SELECT accountnumber, balance FROM Account WHERE Account.balance > 100\"\n}"

headers = {
    'Authorization': "Bearer 6d151216ef504f65b8ff6e9e9e8356d3",
    'content-type': "application/json",
    }

conn.request("POST", "/query/jobs", payload, headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))