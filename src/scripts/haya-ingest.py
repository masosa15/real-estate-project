import requests
from bs4 import BeautifulSoup as Soup
import re
import pandas as pd
import logging
import boto3
import datetime
from io import StringIO


baseurl = "https://www.haya.es/"

def Logger(filename):
    logging.basicConfig(filename=filename,
                        filemode='a',
                        format="%(asctime)s, %(msecs)d %(name)s | %(levelname)s | [ %(filename)s-%(module)s-%(lineno)d ]  : %(message)s",
                        datefmt="%d %H:%M:%S")

    return logging.getLogger()

def get_geo_info():

    apiUrl = "https://www.haya.es/public-assets/index/object/1/"
    querystring = {"geo_province_id":"43",
                   "geo_city_id":"43123",
                   "is_sell_cache":"1",
                   "asset_type_id":"2",
                   "asset_publication_detail_id":"99"}
    payload = ""
    headers = {"cookie": "visid_incap_1561762=rZPnM0cBRtqHbvdFyuNAu2fmRWIAAAAAQUIPAAAAAABnAMBC08Vw8zBewSaIOXlN; incap_ses_315_1561762=4NwIXcu1aTCcXqhq9RpfBGfmRWIAAAAAoASlRvf9Kb29lPYRR6h5Bw%3D%3D"}

    response = requests.request("GET",
                                apiUrl,
                                data=payload,
                                headers=headers,
                                params=querystring)

    return response.json()

def parse_text(text):
    return text


def get_page_soup(url="https://www.haya.es/comprar/viviendas/tarragona/reus/?p=1"):
    # hidden api url in order to put additional filters --> https://www.haya.es/lazy-content/asset/index/index-result/%3Fpagination_random_token=p16488056320510f49f4569fdf1d5057f1814ccd5f0pagination%26asset_selling_mode_id=1%26asset_type_id=2%26geo_province_id=43%26geo_city_id=43123%26asset_publication_detail_id=99%26is_sell_cache=1%26order_by=%5Border_relevance+DESC,haya_asset_identifier+ASC%5D%26p=2/
    response = requests.request("GET", url)

    if not response.ok:  #response.ok -> response.status_code == 200
        log = Logger('../errorLog.log')
        log.info(msg=f"The {url} return a different error code: {response.status_code}")
        print(f"The {url} return a different error code: {response.status_code}")
        return None

    page_soup = Soup(response.text, "html.parser")

    return page_soup

def get_pagination_info(page_soup):

    pagination_items = {}
    pagination = re.compile("haya-pagination *.")
    pagination_items['data-elements-per-page'] = page_soup.main.find("div", {"class": pagination})['data-elements-per-page']
    pagination_items['data-pagination-pages'] = page_soup.main.find("div", {"class": pagination})['data-pagination-pages']
    pagination_items['data-count'] = page_soup.main.find("div", {"class": pagination})['data-count']

    return pagination_items

def get_apartments_url(dataPaginationPages):
    aparment_list = []

    for i in range(1, dataPaginationPages+1):

        page_soup = get_page_soup(f"https://www.haya.es/comprar/viviendas/tarragona/reus/?p={i}")
        h2_filter = re.compile("mb-3 *.")
        urls = page_soup.main.findAll("h2", {"class": h2_filter})
        aparment_list += [url.a['href'] for url in urls]

    return aparment_list

def get_apartments_info(urls_apartment_list):

    aparment_information = []
    for url in urls_apartment_list:
        dic = {"title": "N/A",
             "link": url,
             "description": "N/A",
             "price": "N/A",
             "discount": "N/A",
             "antiguedad": "N/A",
             "superficie_total": "N/A",
             "superficie": "N/A",
             "banos": "N/A",
             "habitaciones": "N/A"
             }

        page_soup = get_page_soup(url)
        if page_soup == None:
            log = Logger('../errorLog.log')
            log.warning(f'Error ocurrido al cargar la pagina : {url}')
            continue

        if(page_soup.find("h1")):
            try:
                dic['title'] = page_soup.find("h1").text
            except:
                dic['title'] = "Error al cargar el titulo del apartamento"


        if(page_soup.main.find("span", {"class": re.compile("badge badge-secondary*.")})):
            discount = page_soup.main.find("span", {"class": re.compile("badge badge-secondary*.")}).text
            dic['discount'] = discount

        price = page_soup.main.find("span", {"class": re.compile("h1*.")}).text
        dic['price'] = price

        if(page_soup.main.find("div", {"class": "col-md-10 mt-3"})):
            description = page_soup.main.find("div", {"class": "col-md-10 mt-3"}).text

        dic['description'] = description

        extra_description = "N/A"
        if page_soup.main.find("div", {"class": "col-sm-10 mt-1"}):
            extra_description=page_soup.main.find("div", {"class": "col-sm-10 mt-1"}).text
            extra_description = parse_text(extra_description)
        dic['extra_description'] = extra_description

        if page_soup.main.find("span", {"text-16 text-book"}):
            address = page_soup.main.find("span", {"text-16 text-book"}).text
        else:
            address = "N/A"
        dic['address'] = address

        caracteristicas = page_soup.main.find("div", {"class": re.compile("row align-items-center")})
        extra_information = []
        for div in caracteristicas.findAll("div", {"class": re.compile("col-4 *.")}):

            if "Antigüedad" in div.text:
                dic["antiguedad"] = div.text
            elif "const" in div.text:
                dic["superficie_total"] = div.text
            elif "útiles" in div.text:
                dic["superficie"] = div.text
            elif "baños" in div.text.lower():
                dic["banos"] = div.text
            elif "hab" in div.text:
                dic["habitaciones"] = div.text
            else:
                extra_information.append(div.text)

        dic['extra_information']= extra_information
        dic['haya_asset_identifier'] = url.split("-")[1].replace("/", "")

        aparment_information.append(dic)
    return aparment_information

def save_aparment_information(df):

    ENDPOINT_URL='http://localhost:4566/'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    today = datetime.datetime.now()
    date_time = today.strftime("%d_%m_%Y_%H:%M")


    filename='scrapped_haya_'+date_time+'.csv'

    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL)
    bucket = s3.Bucket('raw-data')
    response = bucket.put_object(Key=filename, Body=csv_buffer.getvalue())

def main():
    page_soup = get_page_soup()
    pagination_items = get_pagination_info(page_soup)


    urls_apartment_items = get_apartments_url(int(pagination_items['data-pagination-pages']))

    apartment_information = get_apartments_info(urls_apartment_items)
    apartment_df = pd.DataFrame(apartment_information)

    geo_response = get_geo_info()
    geo = [r["Asset"] for r in geo_response['response']["assets"]]
    geo_df = pd.DataFrame(geo)

    result = apartment_df.merge(geo_df, left_on='haya_asset_identifier', right_on='haya_asset_identifier')

    save_aparment_information(result)

if __name__ == '__main__':
        main()
