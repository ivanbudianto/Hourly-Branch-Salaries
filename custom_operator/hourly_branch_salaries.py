import os
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from lib.modules.secret_manager import SecretManager


class HourlyBranchSalaries():
    """
    secret_db_id        : Determines which DB to use
    employees_table     : Employee table to insert the data
    timesheets_table    : Timesheet table to insert the data
    employees_table_primary_key     : PK of employees_table
    timesheets_table_primary_key    : PK of timesheets_table
    employees_csv_file  : Employee CSV file
    timesheets_csv_file : Timesheet CSV file
    result_table        : Result table
    ingest_type         : Whether to append/overwrite the result_table
    sql_path            : Datamart SQL
    """


    def __init__(
                self,
                secret_db_id        = 'talenta_company_1',
                employees_table     = 'employees',
                timesheets_table    = 'timesheets',
                employees_table_primary_key     = 'employee_id',
                timesheets_table_primary_key    = 'timesheet_id',
                employees_csv_file  = None,
                timesheets_csv_file = None,
                result_table        = 'hourly_branch_salaries',
                ingest_type         = 'append',
                sql_path            = None,
                *args,
                **kwargs
                ):

        self.secret_db_id       = secret_db_id
        self.employees_table   = employees_table
        self.timesheets_table = timesheets_table
        self.employees_table_primary_key = employees_table_primary_key
        self.timesheets_table_primary_key = timesheets_table_primary_key
        self.employees_csv_file = employees_csv_file
        self.timesheets_csv_file = timesheets_csv_file
        self.result_table = result_table
        self.ingest_type = ingest_type
        self.sql_path = sql_path


    def update_hourly_branch_salaries(self):
        # Update the hourly_branch_salaries, and commit the changes by using the engine
        with open(self.sql_path, 'r') as file:
            sql_query = file.read()

        sql_query = sql_query.format(employees_table=self.employees_table, timesheets_table=self.timesheets_table)

        with self.engine.connect() as connection:
            sql_query_with_params = text(sql_query)
            connection.execute(sql_query_with_params)
            connection.commit()


    def incremental_ingestion(self, engine, csv_file, table_name, primary_key):
        # Read the new data using Pandas
        new_data = pd.read_csv(csv_file)
        new_data = new_data.sort_values(by=primary_key)
        deduplicated_data = new_data.drop_duplicates(subset=[primary_key], keep='last')

        # Read the existing PostgreSQL table
        existing_data = pd.read_sql(f'SELECT * FROM {table_name}', engine)

        # Distinguish the 'existing' data and 'new' data by adding suffixes on each columns.
        # Then, the data will be merged by using the primary_key.
        existing_data.rename(columns={primary_key: f'{primary_key}_existing'}, inplace=True)
        deduplicated_data.rename(columns={primary_key: f'{primary_key}_new'}, inplace=True)
        merged_data = pd.merge(existing_data, deduplicated_data, how='right', 
                            left_on=f'{primary_key}_existing', right_on=f'{primary_key}_new', 
                            suffixes=('_existing', '_new'))
        for col in merged_data.select_dtypes(include=['float']).columns:
            merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce').astype('Int64')


        # Distinguish the new data by checking whether its primary key exists or not in the existing DB
        new_records = merged_data[merged_data[f'{primary_key}_new'].notna() & merged_data[f'{primary_key}_existing'].isna()]
        new_records = new_records.drop(columns=[col for col in merged_data.columns if col.endswith('_existing')])

        # Check EVERY non-PK columns to check any differences when comparing existing vs new data
        updated_records = merged_data[merged_data.apply(
            lambda row: any(
                str(row[f'{col}_existing']).strip() != str(row[f'{col}_new']).strip()
                if pd.notna(row[f'{col}_existing']) and pd.notna(row[f'{col}_new'])
                else pd.notna(row[f'{col}_existing']) or pd.notna(row[f'{col}_new'])
                for col in deduplicated_data.columns
                if col not in [primary_key, f'{primary_key}_new', f'{primary_key}_existing']
            ),
            axis=1
        )]
        # Deduplicate the new records from updated_records
        updated_records = updated_records[~updated_records[f'{primary_key}_new'].isin(new_records[f'{primary_key}_new'])]
        updated_records = updated_records.drop(columns=[col for col in merged_data.columns if col.endswith('_existing')])

        # Insert new records by appending them into the database
        new_records.rename(columns=lambda col: col.replace('_new', ''), inplace=True)
        if not new_records.empty:
            new_records.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Appended new record: {new_records}")

        # Using dynamic update query to incrementally update the table
        updated_records.rename(columns=lambda col: col.replace('_new', ''), inplace=True)
        if not updated_records.empty:
            for _, row in updated_records.iterrows():
                set_clause = ", ".join([f"{col} = :{col}" for col in updated_records.columns if col != f'{primary_key}'])
                update_query = text(f"""
                    UPDATE {table_name}
                    SET {set_clause}
                    WHERE {primary_key} = :{primary_key};
                """)
                row_dict = row[updated_records.columns].where(pd.notnull(row), None).to_dict()

                with engine.connect() as connection:
                    connection.execute(update_query, parameters=row_dict)
                    connection.commit()

        print(f"Updated new record: {updated_records}")
        return new_records, updated_records


    def execute(self):
        try :
            # Create the engine to access the database using SQLAlchemy
            secret_manager = SecretManager(self.secret_db_id)
            host, port, username, password, dbname = secret_manager.access_secret_by_id()
            print(host, port, username, password, dbname)
            self.engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')

            # Dynamic method calling to do incremental ingestion
            employee_new_records, employee_updated_records = self.incremental_ingestion(self.engine, self.employees_csv_file, self.employees_table, self.employees_table_primary_key)
            timesheet_new_records, timesheet_updated_records = self.incremental_ingestion(self.engine, self.timesheets_csv_file, self.timesheets_table, self.timesheets_table_primary_key)

            # Only update the hourly_branch_salaries table if new records are found
            if len(employee_new_records) != 0 or len(employee_updated_records) != 0 or len(timesheet_new_records) != 0 or len(timesheet_updated_records) != 0:
                self.update_hourly_branch_salaries()

            print("ETL Process has finished successfully")

        except Exception as e:
            raise Exception(' Catch exception on : {} '.format(str(e)))

