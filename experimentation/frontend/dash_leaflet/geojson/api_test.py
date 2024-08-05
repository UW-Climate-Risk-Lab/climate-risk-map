import pgosm_flex_api
from dotenv import load_dotenv
import os

load_dotenv()
PG_DBNAME=os.environ["PG_DBNAME"]
PG_USER=os.environ["PG_USER"]
PG_HOST=os.environ["PG_HOST"]
PG_PASSWORD=os.environ["PG_PASSWORD"]
PG_PORT=os.environ["PG_PORT"]

if __name__=="__main__":

    api = pgosm_flex_api.OpenStreetMapDataAPI(dbname=PG_DBNAME,
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT)
    api.get_osm_data("infrastructure", "power")
    print(api)
