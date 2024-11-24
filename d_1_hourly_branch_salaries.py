import os
from custom_operator.hourly_branch_salaries import HourlyBranchSalaries

TOP_LEVEL_DIR     = os.path.dirname(os.path.abspath(__file__))
BASE_FILE_URL   = os.path.abspath(f"{TOP_LEVEL_DIR}/temp_files")
SQL_PATH        = os.path.abspath(f"{TOP_LEVEL_DIR}/lib/datamart_sql")

if __name__ == "__main__":
    d_1_hourly_branch_salaries_task = HourlyBranchSalaries(
                                        secret_db_id        = 'talenta_company_1',
                                        employees_table     = 'employees',
                                        timesheets_table    = 'timesheets',
                                        employees_table_primary_key     = 'employee_id',
                                        timesheets_table_primary_key    = 'timesheet_id',
                                        employees_csv_file  = f'{BASE_FILE_URL}/employees.csv',
                                        timesheets_csv_file = f'{BASE_FILE_URL}/timesheets.csv',
                                        result_table        = 'hourly_branch_salaries',
                                        ingest_type         = 'append',
                                        sql_path            = f'{SQL_PATH}/hourly_branch_salaries.sql',
                                    )
    
    d_1_hourly_branch_salaries_task.execute()
