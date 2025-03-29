from curl_cffi import requests as cc_requests



def my_custom_request(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
    }

    response = cc_requests.get(url, headers=headers, impersonate="chrome120")
    return response

