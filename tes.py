import requests

url = "https://gateway-voters.eci.gov.in/api/v1/printing-publish/get-part-list"

headers = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,es;q=0.6",
    "Connection": "keep-alive",
    "Origin": "https://voters.eci.gov.in",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
    "applicationname": "VSP",
    "atkn_bnd": "null",
    "channelidobo": "VSP",
    "content-type": "application/json",
    "platform-type": "ECIWEB",
    "rtkn_bnd": "null",
    "sec-ch-ua": "\"Opera GX\";v=\"120\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}

payload = {
    "stateCd": "S24",
    "districtCd": "S2408",
    "acNumber": 86,
    "pageNumber": 0,
    "pageSize": 10
}

response = requests.post(url, headers=headers, json=payload)

print(response.status_code)
print(response.text)
