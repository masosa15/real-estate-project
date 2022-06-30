from bs4 import BeautifulSoup as Soup
from ingest_functions.functions import get_page
from ingest_functions.functions import upload_to_s3

url = "https://www.idealista.com/venta-viviendas/reus-tarragona/con-de-bancos/?ordenado-por=precios-asc"
baseurl = 'https://www.idealista.com'

def get_total_page(html):
    pages = html.findAll("div", {"class": "pagination"})
    return [baseurl+li.a['href'] for li in pages[0].findAll("li", {"class": ""})]

def get_apartment_information(html):

    apartment_list = []
    apartment_items = html.findAll("div", {"class": "item-info-container"})

    for item in apartment_items:
        city = "Reus" #to be modified in the future
        title = item.a['title']
        link = item.a['href']
        reference = link.split("/")[2]
        link = baseurl + link
        address = ""
        price = item.span.text.replace("\n", "").strip()
        description = item.p.text.replace("\n", "")

        #Looking for item information
        item_details = item.findAll("span", {"class": "item-detail"})
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

        apartment_list.append({
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
        })
    return apartment_list

def main():

    page_soup = get_page(url) #getting html of url
    pages_urls = get_total_page(page_soup) # get the total number of pages
    apartments_items = get_apartment_information(page_soup) # using the same request to get the apartments in this page.

    #in the case we have more than 1 page
    if len(pages_urls) > 1:

        for page in pages_urls:
            page_soup = get_page(page)
            apartments_items.extend(get_apartment_information(page_soup))

    upload_to_s3(apartments_items, 'scrapped_idealista_')

if __name__ == '__main__':
    main()