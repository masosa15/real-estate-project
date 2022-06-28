from urllib.request import urlopen
from bs4 import BeautifulSoup as Soup
import re
import ssl
import pandas as pd
import math
import boto3
import datetime
from io import StringIO

baseurl = "https://www.inmobiliariabancaria.com"
secondary_url ="https://www.inmobiliariabancaria.com/pisos-venta/reus/index/orden/precio/"
context = ssl._create_unverified_context()

# Have to be implemented into a csv to be common to every script
city_list = ['reus', 'tarragona']

def get_page(url, context = context):
    page_html = urlopen(url, context=context).read().decode("utf-8")
    page_soup = Soup(page_html, "html.parser")
    return page_soup

def get_apartment_information_by_page(url, city):

    page_soup = get_page(url)
    regex = re.compile("card.*")
    apartments = page_soup.findAll("div", {"class": regex})

    total_apartment_list =[]
    for container in apartments:

        link = baseurl + container.a["href"]
        title = container.img["title"]

        address, reference = container.p.text.split("Ref.", 2)
        address = address.strip()
        reference = reference.strip()

        description = container.findAll("p", {"class": "text-overflow"})
        description = description[0].text.strip()

        price = container.findAll("div", {"class": "h2 text-primary pull-right"})
        price = price[0].text.strip()

        extra_info = container.findAll("li")

        try:
            superficie = extra_info[0].text
        except IndexError:
            superficie = 'None'
        try:
            habitaciones = extra_info[1].text
        except IndexError:
            habitaciones = 'None'
        try:
            banos = extra_info[2].text
        except IndexError:
            banos = 'None'

        apartments_dic = {
            "city": city,
            "title": title,
            "link": link,
            "reference": reference,
            "address": address,
            "price": price,
            "supercifie": superficie,
            "habitaciones": habitaciones,
            "baños": banos}
        total_apartment_list.append(apartments_dic)

    return total_apartment_list

def get_total_pages_number(url):
    page_soup = get_page(url)
    res = [int(i) for i in str(page_soup.h2.small) if i.isdigit()]
    res = int(''.join(map(str, res)))
    total_page_number = math.ceil(res/12)

    return total_page_number

def get_cities_urls(city_list):
    urls = [f"https://www.inmobiliariabancaria.com/pisos-venta/{x}/index/orden/precio/" for x in city_list]
    return urls

def save_csv(results):
    df = pd.DataFrame(results)

    ENDPOINT_URL = 'http://localhost:4566/'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)


    today = datetime.datetime.now()
    date_time = today.strftime("%d_%m_%Y_%H:%M")


    filename='scrapped_inmobiliariabancaria_'+date_time+'.csv'
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL)
    bucket = s3.Bucket('raw-data')
    bucket.put_object(Key=filename, Body=csv_buffer.getvalue())

def main():
    cities_page_list = get_cities_urls(city_list)
    item_list = []

    for x, city_page in enumerate(cities_page_list):
        url = city_page
        city = city_list[x]
        total_pages = get_total_pages_number(url)
        #Pagination
        for i in range(1, total_pages+1):
            item_list += get_apartment_information_by_page(url+f"page/{i}", city)

    save_csv(item_list)


if __name__ == '__main__':
    main()