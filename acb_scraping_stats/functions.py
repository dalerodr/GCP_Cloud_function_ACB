import os
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

# base url to get results
base_url = 'http://www.acb.com/partido/estadisticas/id/'



def games_to_scrape(batch):
   """ 
   - Returns:
      match_id to scrape.        
   """

   client = bigquery.Client()

   print("Client creating using default project: {}".format(client.project))

   query = """
      SELECT match_id
      FROM `%s.%s.%s`
      where batch = %s """ %(os.environ.get("PROJECT_NAME"), os.environ.get("DATASET_STAGING"), os.environ.get("TABLE_SCRAP_GAMES"), batch)

   query_job = client.query(query)

   df = query_job.to_dataframe()

   return df



def scraping_data_acb(batch):
   """ 
   Scraping ACB data.
   """

   localidad = 'home'
   num = 1
   jornada = 1
   basket_data = []

   # scraping games
   games = games_to_scrape(batch)

   for match_id in games['match_id']:
      
      print('Partido id: ' + str(match_id))

      url = base_url + str(match_id)

      ## our_url = http.request('GET', url)
      ## contenidoWeb = our_url.data
      ## contenidoWeb = contenidoWeb.decode(encoding='utf-8', errors='strict')
      ## soup = BeautifulSoup(contenidoWeb, 'html.parser')

      req = requests.get(url)
      html_string = str(req.content, "utf-8")
      soup = BeautifulSoup(html_string, features="html.parser")
      players = []
      headers = ['dorsal_number','name','minutes','points','t2','t2_percent','t3','t3_percent','t1','t1_percent','rebounds','defensive_ofensive','assists','steals','turnovers','fast_breaks','defensive_blocks','blocks_received','dunks','personal_fouls', 'fouls_received', 'plus_minus','player_efficiency_rating']

      for i in soup.find_all('section', 'partido'):
         rows = i.find_all('tr')
         team = i.find('h6')
         team = team.text.replace('&nbsp;', '').replace('\xa0', ' ')
         team = team.split(' ')
         team = team[0:2]
         for row in rows:
               data = row.find_all('td')
               player = []
               for d in data:
                  player.append(d.text)
               players.append(player)
         players = players[2:len(players)-2]
         players = [headers] + players

         if num > 18:
               num = 1
               jornada += 1
         
         player_stats = pd.DataFrame(players, columns = headers)
         player_stats["gameday"] = jornada
         player_stats["home_away"] = localidad
         player_stats["game_id"] = match_id
         player_stats = player_stats[:-2].drop(0)

         basket_data.append(player_stats)

         ## file_name = folder + team[0].lower() + '_' + team[1].lower() + '_' + str(jornada) + '.csv'
         ## np.savetxt(file_name,  
         ##    players, 
         ##    delimiter =", ",  
         ##    fmt ='% s') 

         num += 1
         players = []

         if localidad == 'home':
               localidad = 'away'
         else:
               localidad = 'home'

   basket_data = pd.concat(basket_data, sort=False, ignore_index='True')

   return basket_data



def clean_data(data):
   """ 
   Clean functions 
   """

   vars = data.columns
   for variable in vars:
      data[variable] = data[variable].astype(str).str.strip()
   
   # Arreglos: extraemos numeros, calculamos minutos exactos de juego, quitamos signos extraños
   data['lineup'] = np.where(data['dorsal_number'].str.contains('\*'), 'starter', 'bench')
   data['dorsal_number'] = data['dorsal_number'].str.replace(r'\D', '', regex=True)

   data['seconds'] = data['minutes'].str.split(pat=':',expand=True)[1]
   data['seconds'] = pd.to_numeric(data['seconds'], errors='coerce').astype('float') / 60
   data['minutes'] = data['minutes'].str.split(pat=':',expand=True)[0]
   data['minutes'] = pd.to_numeric(data['minutes'], errors='coerce').astype('float') + data['seconds']

   data['t2_percent'] = data['t2_percent'].str.replace(r'\D', '', regex=True)
   data['t3_percent'] = data['t3_percent'].str.replace(r'\D', '', regex=True)
   data['t1_percent'] = data['t1_percent'].str.replace(r'\D', '', regex=True)

   data['t1_made'] = data['t1'].str.split(pat='/',expand=True)[0]
   data['t1_attempted'] = data['t1'].str.split(pat='/',expand=True)[1]

   data['t2_made'] = data['t2'].str.split(pat='/',expand=True)[0]
   data['t2_attempted'] = data['t2'].str.split(pat='/',expand=True)[1]

   data['t3_made'] = data['t3'].str.split(pat='/',expand=True)[0]
   data['t3_attempted'] = data['t3'].str.split(pat='/',expand=True)[1]

   data['defensive_rebounds'] = data['defensive_ofensive'].str.split(pat='+',expand=True)[0]
   data['ofensive_rebounds'] = data['defensive_ofensive'].str.split(pat='+',expand=True)[1]

   
   # Transformación de types a int y float
   variables_int = ['dorsal_number','points','rebounds','assists','steals','turnovers','fast_breaks','defensive_blocks','blocks_received','dunks','personal_fouls','fouls_received','gameday','game_id','t1_made','t1_attempted','t2_made','t2_attempted','t3_made','t3_attempted','defensive_rebounds','ofensive_rebounds']
   for var in variables_int:
      data[var] = pd.to_numeric(data[var], errors='coerce').astype('Int64')
   
   variables_float = ['t2_percent','t3_percent','t1_percent','plus_minus','player_efficiency_rating']
   for var in variables_float:
      data[var] = pd.to_numeric(data[var], errors='coerce').astype('float')

   
   # Select de las columnas que vamos a utilizar
   data = data[['dorsal_number', 'lineup', 'name', 'minutes', 'points', 't2_made','t2_attempted', 't2_percent', 't3_made', 't3_attempted', 't3_percent', 
                  't1_made', 't1_attempted', 't1_percent', 'rebounds', 'defensive_rebounds', 'ofensive_rebounds', 'assists', 'steals', 'turnovers', 
                  'fast_breaks', 'defensive_blocks', 'blocks_received', 'dunks', 'personal_fouls', 'fouls_received', 'plus_minus', 'player_efficiency_rating', 'gameday', 'home_away', 'game_id']]

   return data



def insert_BQ(client, data):
   """ 
   - Insert data in BQ.        
   """

   # The project defaults to the Client's project if not specified.
   dataset = client.get_dataset("{}.{}".format(os.environ.get("PROJECT_NAME"), os.environ.get("DATASET_SOURCES")))

   table_ref = dataset.table(os.environ.get("TABLE_STATS_SCRAP_GAMES"))

   job_config = bigquery.job.LoadJobConfig()
   # bigquery.WriteDisposition --> https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.WriteDisposition
   # The default value is WRITE_APPEND
   job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
   

   job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
   
   job.result()  # Waits for table load to complete.
   
   return("Loaded dataframe to {}".format(table_ref.path))
