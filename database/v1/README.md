Contains database setup and migrations for v1 of the Climate Risk Map API

Databases are populated initially with the PgOSM Flex ETL Tool. Databases are segmented by region. This is done because once a database is populated with data from the ETL tool for a given region, it becomes difficult to switch regions. This should also allow for more efficient queries by region.

## Database Setup

1. **Initialize the Database**:
   Run the `init_db.sql` script to create the database, roles, and schemas. The region name provided will be the name of the database.
   ```sh
   psql -v region_name=<name> -f /path/to/database/v1/init_db.sql
   ```

2. **Run Migrations**:
   Use the `run_migrations.sh` script to execute all migration scripts in the `migrations` directory.
   ```sh
   ./run_migrations.sh
   ```

3. **Create Materialized Views**:
   Run the scripts to create materialized views.
   ```sh
   psql -f /path/to/database/v1/views/20250122_create_place_materialized_view.sql
   psql -f /path/to/database/v1/views/20250122_create_landuse_materialized_view.sql
   psql -f /path/to/database/v1/views/20250122_create_amenity_materialized_view.sql
   psql -f /path/to/database/v1/views/20240912181938_create_infrastructure_materialized_view.sql
   psql -f /path/to/database/v1/views/20250124_create_nasa_nex_fwi_metadata_materialized_view.sql
   ```

4. **Refresh Materialized Views**:
   Use the `refresh_materialized_views.sql` script to refresh all materialized views.
   ```sh
   psql -f /path/to/database/v1/views/refresh_materialized_views.sql
   ```

## Notes

- Ensure that the roles and permissions are correctly set up as per the `init_db.sql` script.
- Materialized views should be refreshed periodically to ensure data consistency.
- Ensure that the `.env` file is correctly set up with the necessary database connection details before running the `run_migrations.sh` script.