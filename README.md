# Data Engineering Project (WIP)

For this Data Engineering project we are using the following technologies and tools:

+ **Stream:** Scrape websites using BeautifulSoup for new properties listing. The scripts run daily using airflow Dags **-> Complete**
+ **Orchestrator and Scheduler:** Airflow DAGs **-> Complete**
+ **Store:** Store the scrapped information for each site in S3. For the purpose of this project we use localstack. Localstacks provides the same functionality and APIs as the real AWS cloud environment. **-> Complete **
+ **Structure:** Parse the data stored in S3. Use spark to load data into postgres - Working in progress
+ **Show:** Dashboards using Tableau

Data Architecture:


![DE Project drawio](https://user-images.githubusercontent.com/102515224/173576883-3ff66e06-5656-4d49-a681-792688b0f889.png)
