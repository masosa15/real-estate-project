import requests

def get_proxies(protocol='http'):

    url = f"https://api.proxyscrape.com/v2/?request=displayproxies&protocol={protocol}&timeout=10000&country=all&ssl=all&anonymity=all"
    response = requests.get(url)

    return response.text.split("\r\n")


