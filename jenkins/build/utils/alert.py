import requests

def line_notify(token,msg):
    try:
        url = 'https://notify-api.line.me/api/notify'
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
        r = requests.post(url, headers=headers, data = {'message':msg})
        return r.text
    except Exception as e:
        return e

if __name__ == "__main__":
    import constant
    value = line_notify("yixwt2Mpe0I4v7WWWrLV2VXmzgTg1e3AWa6RFj6YhQB","hello")
    print(value)