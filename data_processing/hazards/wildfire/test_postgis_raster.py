import os
import re
import subprocess
import psycopg2

PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = "climate_user"
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]

def load_geotiff_postgis(tiff_files, ssp):
    
    conn = psycopg2.connect(database=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, port=PG_PORT, host=PG_HOST)
    conn.autocommit = True
    cur = conn.cursor()

    # Optionally create the table schema if not done yet.
    # with open("create_nasa_nex_fwi.sql", "r") as f:
    #     sql_script = f.read()
    # cur.execute(sql_script)

    # Find all TIFF files matching the pattern XXXX_XX_washington.tif
    # e.g., fwi_2030_7.tif
    filename_regex = r"^(\d+)-(\d+)-([^.]+)\.tif$"

    for tiff_path in tiff_files:
        filename = os.path.basename(tiff_path)
        match = re.match(filename_regex, filename)
        if not match:
            print(f"Skipping file (doesn't match pattern): {filename}")
            continue

        # Parse decade, month, state from file name
        decade_str, month_str, state_str = match.groups()
        decade = int(decade_str)
        month = int(month_str)

        print(f"Processing TIFF: {filename} => decade={decade}, month={month}, ssp={ssp}")

        # 2. Get the current max rid (so we know which new rows appear after import)
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM climate.nasa_nex_fwi_raster")
        before_max_rid = cur.fetchone()[0]

        # 3. Run raster2pgsql in append mode (-a)
        #    -s 4326  => sets the source SRID to EPSG:4326 (change if needed)
        #    -t 1x1   => Each row is a single grid cell
        #    -I       => create a GIST index on the raster column (optional, or run once with -c)
        #    -C       => apply VACUUM ANALYZE
        #    -N 0     => set 0 as NODATA value for all bands (adjust if needed)
        #    -a       => append to existing table
        #    climate.nasa_nex_fwi_raster => the target table in Postgres
        #
        #    We pipe the generated SQL directly into psql.
        raster2pgsql_cmd = [
            "raster2pgsql",
            "-s", "4326",
            "-t", "1x1",
            "-I",
            "-C",
            "-N", "0",
            "-a",
            tiff_path,
            "climate.nasa_nex_fwi_raster"
        ]

        # We'll call psql with the necessary connection info:
        # Example: psql -h HOST -p PORT -U USER -d DBNAME
        psql_cmd = [
            "psql",
            "-h", PG_HOST,
            "-p", str(PG_PORT),
            "-U", PG_USER,
            "-d", PG_DBNAME
        ]

        print("Running raster2pgsql + psql...")
        # We run raster2pgsql, pipe its stdout to psql
        raster2pgsql_process = subprocess.Popen(
            raster2pgsql_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        psql_process = subprocess.run(
            psql_cmd,
            stdin=raster2pgsql_process.stdout,
            text=True
        )

        # Close raster2pgsql_process (to capture any errors)
        raster2pgsql_process.stdout.close()
        stderr_data = raster2pgsql_process.stderr.read().strip()
        raster2pgsql_returncode = raster2pgsql_process.wait()

        if raster2pgsql_returncode != 0:
            print(f"ERROR running raster2pgsql for {filename}: {stderr_data}")
            continue
        else:
            print(f"raster2pgsql loaded {filename} successfully.")

        # 4. Now, find the max rid after loading
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM climate.nasa_nex_fwi_raster")
        after_max_rid = cur.fetchone()[0]

        if after_max_rid > before_max_rid:
            # 5. Update newly inserted rows to include decade, month, ssp
            update_sql = """
                UPDATE climate.nasa_nex_fwi_raster
                SET decade = %s,
                    month  = %s,
                    ssp  = %s
                WHERE id > %s
                  AND id <= %s
            """
            cur.execute(update_sql, (decade, month, ssp, before_max_rid, after_max_rid))
            print(f"Updated rows from id>{before_max_rid} to id<={after_max_rid} with decade={decade}, month={month}, ssp={ssp}.")
        else:
            print("No new rows inserted? Check if tiling or table structure caused an issue.")

    cur.close()
    conn.close()

if __name__=="__main__":
    tiff_files = [""]
    ssp=370