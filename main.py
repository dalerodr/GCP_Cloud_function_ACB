import functions_framework
from acb_scraping_stats.functions import scraping_data_acb, clean_data, insert_BQ
from google.cloud import bigquery

def main(request):
   request_json = request.get_json(silent=True)
   
   if request_json and "batch" in request_json:
      batch = request_json["batch"]
   else:
      raise ValueError("JSON is invalid, or missing a 'batch' property")

   client = bigquery.Client()
   
   results = scraping_data_acb(batch)
   data = clean_data(results)
   insert_BQ(client, data)

   return('Scraping games succesfully')
