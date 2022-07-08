# Data Engineering Project

**Introduction**

The main goal for this project is to get the information of discounted properties in differents websites and compare it to approximate market value. In order to comparate to the approximate market value, im going to use the BBVA apraiser scrapper to get all the relative market information. 

For the presentation layer, i'll use tableu dashboard to highlight which are the properties with better investment oportunities.

**Data Architecture:**

The data source come from scrappers made to extract the information in 4 differents properties listing sites. Airflows orchestrates each proccess to upload the raw data to a S3 bucket. Once the raw-data is stored the cleaning proccess start to parse and remove duplicates beetween sites. The cleaned data is store in a different bucket.

Once we have the list of discounted properties, we find the relative market value of each property using the BBVA appraiser application. We use a Spark to upload the data into amazon RDS(posgres).

Finally, we use tableu as for the presentation layer and highlight the properties with better investment oportunities.

![architecture-recortada](https://user-images.githubusercontent.com/102515224/177830368-dcab8e8d-b830-491d-8b31-961469d160a0.png)


Summary:

+ **Stream:** Scrape websites using BeautifulSoup for new properties listing. The scripts run daily using airflow as an orchestator **
+ **Orchestrator and Scheduler:** Airflow DAGs **
+ **Store:** Store the scrapped information for each site in S3. For the purpose of this project we use localstack. Localstacks provides the same functionality and APIs as the real AWS cloud environment. **
+ **Structure:** Parse the data stored in S3. Use spark to load data into AMAZON RDS (postgres)
+ **Show:** Dashboards using Tableau


**Snowflake model - Database**

The data model is implemented using a snowflake schema with bridge tables to make many to many relationship.

The fact table implemented is : `fact_property` 

![database - real_estate_project-Page-1 drawio](https://user-images.githubusercontent.com/102515224/177783719-3f83a6ad-c754-4944-9e32-0abc422ad2e4.png)
