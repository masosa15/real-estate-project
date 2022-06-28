from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import re
import pandas as pd
import math
from io import StringIO
import boto3

import datetime


baseurl = "https://www.outletdeviviendas.com"
context = ssl._create_unverified_context()
url = ("https://www.outletdeviviendas.com/comprar-viviendas-bancos/tarragona/[*city*]/pagina_[*number*]?ord=precio_asc")
city_list = ['reus', 'tarragona']
total_apartment_list = []
items_per_page = 24

def get_page_soup(url):
    page_html = urlopen(url, context=context).read()
    page_soup = BeautifulSoup(page_html, "html.parser")
    return page_soup

def get_total_page_number(url):
    """**TO BE implemented**"""
    page_soup = get_page_soup(url.replace("[*number*]", "1"))
    page_number = page_soup.h1.text.split(" ", 1)[0]
    page_number = math.ceil(int(page_number)/items_per_page)
    return page_number

def get_page_list(url, city_list):
    url_city = [url.replace("[*city*]", x) for x in city_list]

    total_url_list = []
    for x in url_city:
        page_number = get_total_page_number(x)

        for j in range(1, page_number+1):
            total_url_list.append(x.replace("[*number*]", str(j)))

    return total_url_list

def get_apartment_by_page(page_soup, city):
    re_inmu = re.compile("inmu-dest.*")
    divs = page_soup.body.findAll("div", {"class": re_inmu})
    atributos = []

    for div in divs:

        atributos = div.a['title'].split(",")
        city = city
        title = atributos[0]
        link = baseurl + div.a['href']
        reference = div.a['href'].split("/")[2]

        if("€" in atributos[3]):
            address = atributos[2]
            price = atributos[3]

            try:
                superficie = atributos[6]
            except:
                superficie = ""

            try:
                habitaciones = atributos[4]
            except:
                habitaciones = ""

            try:
                banos = atributos[5]
            except:
                banos = ""
        else:
            address = atributos[3]
            price = atributos[4]
            try:
                superficie = atributos[7]
            except:
                superficie = ""

            try:
                habitaciones = atributos[5]
            except:
                habitaciones = ""

            try:
                banos = atributos[6]
            except:
                banos = ""

        apartment = {
            "city": city,
            "title": title,
            "link": link,
            "reference": reference,
            "address": address,
            "price": price,
            "superficie": superficie,
            "habitaciones": habitaciones,
            "banos": banos
       }
        total_apartment_list.append(apartment)

    return total_apartment_list

def save_csv(results):
    df = pd.DataFrame(results)

    ENDPOINT_URL = 'http://localhost:4566/'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    today = datetime.datetime.now()
    date_time = today.strftime("%d_%m_%Y_%H:%M")

    filename = 'scrapped_OV_' + date_time + '.csv'
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL)
    bucket = s3.Bucket('raw-data')
    bucket.put_object(Key=filename, Body=csv_buffer.getvalue())


def main():
    total_url_list = get_page_list(url, city_list)

    for urls in total_url_list:
        page_soup = get_page_soup(urls)
        city = urls.split("/pagina", 1)[0].split("/tarragona", 1)[1].strip("/")
        get_apartment_by_page(page_soup, city)
    save_csv(total_apartment_list)


if __name__ == '__main__':
    main()