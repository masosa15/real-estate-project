import requests
from bs4 import BeautifulSoup as Soup
import pandas as pd
import boto3
import datetime
from io import StringIO

baseurl = "https://www.idealista.com"
apiurl = "https://www.idealista.com/ajax/listingcontroller/listingajax.ajax"

def get_page_data(page = 1):
    querystring = {"locationUri": "reus-tarragona",
                   "page": f"{page}",
                   "typology": "1",
                   "operation": "1",
                   "freeText": "",
                   "adfilter_pricemin": "default",
                   "adfilter_price": "default",
                   "adfilter_area": "default",
                   "adfilter_areamax": "default",
                   "adfilter_countryhouses": "",
                   "adfilter_rooms_0": "",
                   "adfilter_rooms_1": "",
                   "adfilter_rooms_2": "", "adfilter_rooms_3": "", "adfilter_rooms_4_more": "", "adfilter_baths_1": "",
                   "adfilter_baths_2": "", "adfilter_baths_3": "", "adfilter_newconstruction": "",
                   "adfilter_goodcondition": "", "adfilter_toberestored": "", "adfilter_hasairconditioning": "",
                   "adfilter_wardrobes": "", "adfilter_lift": "", "adfilter_flatlocation": "",
                   "adfilter_parkingspace": "", "adfilter_garden": "", "adfilter_swimmingpool": "",
                   "adfilter_hasterrace": "", "adfilter_boxroom": "", "adfilter_accessibleHousing": "",
                   "adfilter_top_floor": "", "adfilter_intermediate_floor": "", "adfilter_ground_floor": "",
                   "adfilter_hasplan": "", "adfilter_digitalvisit": "", "adfilter_agencyisabank": "1",
                   "adfilter_published": "default", "ordenado-por": "precios-asc", "adfilter_onlyflats": "",
                   "adfilter_penthouse": "", "adfilter_duplex": "", "adfilter_homes": "", "adfilter_independent": "",
                   "adfilter_semidetached": "", "adfilter_terraced": "", "adfilter_chalets": ""}
    payload = ""
    headers = {
        "cookie": "cookieSearch-1=%22%2Fventa-viviendas%2Freus-tarragona%2Fcon-de-bancos%2F%3A1648547056166%22; datadome=jZCbliOG7J82zSaaeq-2YBuYzgdH01fF8FGDLT5aqe1tbiXJu-Z-FbDP.gs0oZk-qNSFJaHww1NZMi0ELms8INqEMcvC612VDYfKNWmey1Eh8yZnz9JOZ8J47~VN2F6; SESSION=ef256b4791da9513~0ebebe00-69e7-43c2-85a7-3f4a0d1c3fb9; contact0ebebe00-69e7-43c2-85a7-3f4a0d1c3fb9=%22%7B'email'%3Anull%2C'phone'%3Anull%2C'phonePrefix'%3Anull%2C'friendEmails'%3Anull%2C'name'%3Anull%2C'message'%3Anull%2C'message2Friends'%3Anull%2C'maxNumberContactsAllow'%3A10%2C'defaultMessage'%3Atrue%7D%22",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:91.0) Gecko/20100101 Firefox/91.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": "https://www.idealista.com/venta-viviendas/reus-tarragona/con-de-bancos/",
        "Cookie": "userUUID=1fbf2dea-4d87-46a0-972e-1ed0dcdb2282; SESSION=ef256b4791da9513~e52be846-0589-4115-a488-050d6a1a103a; datadome=pophRBcIfj9h4xN7ECWoLcvKvQRAP7OmNEihM~XpdbKLR5OW6bi9H05qowMx~bksxW-.vdJ95Clu9eYqClIJWl.bkDQKutX2LciD2deZP-Onud9bP-nNsNPsUzSCapD; utag_main=v_id:017fd05ed0bb003aa95059c7bf9200052001d00f0093c$_sn:4$_se:11$_ss:0$_st:1648548780659$dc_visit:4$ses_id:1648545692683%3Bexp-session$_pn:6%3Bexp-session$_prevVtSource:directTraffic%3Bexp-1648549293306$_prevVtCampaignCode:%3Bexp-1648549293306$_prevVtDomainReferrer:%3Bexp-1648549293306$_prevVtSubdomaninReferrer:%3Bexp-1648549293306$_prevVtUrlReferrer:%3Bexp-1648549293306$_prevVtCampaignLinkName:%3Bexp-1648549293306$_prevVtCampaignName:%3Bexp-1648549293306$_prevVtRecommendationId:%3Bexp-1648549293306$_prevCompletePageName:11%3A%3Alistado%3A%3Aordenar%3A%3Abaratos%3Bexp-1648550580663$_prevLevel2:11%3Bexp-1648550580663$dc_event:7%3Bexp-session$dc_region:eu-central-1%3Bexp-session$_prevCompleteClickName:; didomi_token=eyJ1c2VyX2lkIjoiMTdmZDA1ZWQtMjA1Mi02NGEwLWJkYmMtZDQ4NDRjMjY5MzI0IiwiY3JlYXRlZCI6IjIwMjItMDMtMjhUMTE6NTI6MzEuMzczWiIsInVwZGF0ZWQiOiIyMDIyLTAzLTI4VDExOjUyOjMxLjM3M1oiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzptaXhwYW5lbCIsImM6YWJ0YXN0eS1MTGtFQ0NqOCIsImM6aG90amFyIiwiYzp5YW5kZXhtZXRyaWNzIiwiYzpiZWFtZXItSDd0cjdIaXgiLCJjOmFwcHNmbHllci1HVVZQTHBZWSIsImM6dGVhbGl1bWNvLURWRENkOFpQIiwiYzppZGVhbGlzdGEtTHp0QmVxRTMiLCJjOmlkZWFsaXN0YS1mZVJFamUyYyJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJhbmFseXRpY3MtSHBCSnJySzciLCJnZW9sb2NhdGlvbl9kYXRhIl19LCJ2ZXJzaW9uIjoyLCJhYyI6IkFGbUFDQUZrLkFBQUEifQ==; atuserid=%7B%22name%22%3A%22atuserid%22%2C%22val%22%3A%22034a7ef6-4b86-4e85-8a24-21f82c80f9e0%22%2C%22options%22%3A%7B%22end%22%3A%222023-04-29T11%3A52%3A30.475Z%22%2C%22path%22%3A%22%2F%22%7D%7D; atidvisitor=%7B%22name%22%3A%22atidvisitor%22%2C%22val%22%3A%7B%22vrn%22%3A%22-582065-%22%7D%2C%22options%22%3A%7B%22path%22%3A%22%2F%22%2C%22session%22%3A15724800%2C%22end%22%3A15724800%7D%7D; euconsent-v2=CPWickAPWickAAHABBENCECoAP_AAAAAAAAAF5wBAAIAAtAC2AvMAAABAaADAAEESyUAGAAIIllIAMAAQRLIQAYAAgiWOgAwABBEsJABgACCJYyADAAEESxUAGAAIIlg.f_gAAAAAAAAA; ABTasty=uid=dbt5bqqqa5cy4502&fst=1648468351723&pst=1648476402954&cst=1648545693288&ns=4&pvt=18&pvis=11&th=; _gcl_au=1.1.164832624.1648468361; afUserId=d7253c59-d223-4302-bf5c-5a79f296dd5f-p; _fbp=fb.1.1648468362447.554175054; _hjSessionUser_250321=eyJpZCI6IjA5ZWM3ZDAwLTg2OGQtNTZmNC1iZTVkLWFiZDg1MTA5MTNlNCIsImNyZWF0ZWQiOjE2NDg0NjgzNjI1MDIsImV4aXN0aW5nIjp0cnVlfQ==; _hjCachedUserAttributes=eyJhdHRyaWJ1dGVzIjp7ImlkX3BhZ2VMYW5ndWFnZSI6ImVzIiwiaWRfdXNlclJvbGUiOiIifSwidXNlcklkIjpudWxsfQ==; AF_SYNC=1648468362584; TestIfCookie=ok; TestIfCookieP=ok; pbw=%24b%3d12910%3b%24o%3d12100%3b%24sw%3d1280%3b%24sh%3d768; vs=33114=4859143; pid=4644296056139835466; sasd2=q=%24qc%3D1312104537%3B%24ql%3DMedium%3B%24qpc%3D43201%3B%24qt%3D228_3452_84453t%3B%24dma%3D0&c=1&l=1221249316&lo=1444610884&lt=637840723693692052&o=1; sasd=%24qc%3D1312104537%3B%24ql%3DMedium%3B%24qpc%3D43201%3B%24qt%3D228_3452_84453t%3B%24dma%3D0; cto_bundle=FwCLuF8ycmZjSU1RN2hJT0lyQThvbmRIcGRBZzVnSXZWaGZkTGhHaE91WkVuQkVmZ2xJWWRuMjhxNjM5SjIybE1peDA3JTJGVGNWJTJCNCUyQiUyRlhXNHRzdUJRUXRtbjUlMkZWekNjREJKNTZ4QmlIblhCajk2MGdOOTJXT3B2bGhtNCUyQjBiZFdYQTQya2R2ZEljWnM4ZndiMmxDMllyVkZta2QxZ3N0b3k3dHVVNlB5bUhBa01KZiUyRkVxaVNmc3cwRG5QTnFmc040ZEFVNw; askToSaveAlertPopUp=true; lcsrd=2022-03-28T11:54:09.9439376Z; contacte52be846-0589-4115-a488-050d6a1a103a=\"{email':null,'phone':null,'phonePrefix':null,'friendEmails':null,'name':null,'message':null,'message2Friends':null,'maxNumberContactsAllow':10,'defaultMessage':true}; ABTastySession=mrasn=&sen=10&lp=https%253A%252F%252Fwww.idealista.com%252Fen%252Fventa-viviendas%252Freus-tarragona%252Fcon-de-bancos%252F%253Fordenado-por%253Dprecios-asc; dyncdn=6; _hjIncludedInSessionSample=0; _hjSession_250321=eyJpZCI6IjI1Y2I2NGM1LTFiZjctNDZhNC1hM2I1LTY0ZGQxYzM5ZmQ0MSIsImNyZWF0ZWQiOjE2NDg1NDU2OTQ3NjcsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=0; sende52be846-0589-4115-a488-050d6a1a103a={'friendsEmail':null,'email':null,'message':null}; listingGalleryBoostEnabled=false; cnfq=1; cookieSearch-1=/venta-viviendas/reus-tarragona/con-de-bancos/:1648546979017'",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "TE": "trailers"
    }

    response = requests.request("GET", apiurl, data=payload, headers=headers, params=querystring)
    data = response.json()

    return data

def get_total_page_information():

    data = get_page_data()
    total_pages = data['jsonResponse']['totalPages']
    total_items = data['jsonResponse']['listingTotalResults']

    return [total_pages, total_items]

def get_apartment_information(total_pages):

    apartment_list = []
    for i in range(1, total_pages+1):

        data = get_page_data(page=i)
        html = Soup(data['plainText'], "html.parser")
        apartment_items = html.findAll("div", {"class": "item-info-container"})

        for items in apartment_items:
            city = "Reus" #to be modified in the future
            title = items.a['title']
            link = items.a['href']
            reference = link.split("/")[2]
            link = baseurl + link
            address = ""
            price = html.span.text.replace("\n", "").strip()
            description = html.p.text
            #Looking for item information
            item_details = html.findAll("span", {"class": "item-detail"})
            try:
                habitaciones = item_details[0].text
            except:
                habitaciones=""
            try:
                superficie = item_details[1].text
            except:
                superficie = ""
            try:
                extra = item_details[2].text
            except:
                extra = ""
            baños = ""

            dic = {
                "city": city,
                "title": title,
                "link": link,
                "reference": reference,
                "address": address,
                "price": price,
                "superficie": superficie,
                "habitaciones": habitaciones,
                "baños": baños,
                "extra": extra,
                "description": description
            }
            apartment_list.append(dic)
    return apartment_list

def save(results):
    df = pd.DataFrame(results)

    ENDPOINT_URL = 'http://localhost:4566/'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    today = datetime.datetime.now()
    date_time = today.strftime("%d_%m_%Y_%H:%M")

    filename = 'scrapped_idealista_' + date_time + '.csv'
    s3 = boto3.resource('s3', endpoint_url=ENDPOINT_URL)
    bucket = s3.Bucket('raw-data')
    bucket.put_object(Key=filename, Body=csv_buffer.getvalue())


def main():
    total_pages, total_items = get_total_page_information()
    apartment_list = get_apartment_information(total_pages)

    if len(apartment_list) == int(total_items):
        print("Se han obtenido todos los apartamentos con el filtro de busqueda seleccionado")
    else:
        print(f"""Ha ocurrido un error al obtener la información 
                total_items: {total_items} != total de items obtenidos: {len(apartment_list)}""")

    save(apartment_list)

if __name__ == '__main__':
    main()