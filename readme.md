<p align='center'>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  </a>
  <a href="https://www.postgresql.org/">
    <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white"/>
  </a>
</p>
<img src="readme_asset/freepik_hero_image.jpeg" alt="People looking at documents" style="zoom: 67%;" />

# Project Hourly Branch Salaries

Author: Ivan Budianto

<div id="toc">

**Table of Contents**

[Project Hourly Branch Salaries](#project-hourly-branch-salaries)  
 **[I. Project Overview](#i-project-overview)**  
  [Dataset(s) currently on possesion](#datasets-currently-on-possesion)  
   [1. Employees](#1-employees)  
   [2. Timesheets](#2-timesheets)  
  [Objective](#objective)   

  **[II. Assumptions, Limitations, and Edge Cases](#ii-assumptions-limitations-and-edge-cases)**  

 **[ III. Solution & Result Overview](#iii-solution--result-overview)**  
  [a. Full-snapshot SQL script](#a-full-snapshot-sql-script)  
  [b. Incremental Ingestion Python script](#b-incremental-ingestion-python-script)  

  **[IV. Solution & Result Logic Detail - Full-snapshot SQL script](#iv-solution--result-logic-detail---full-snapshot-sql-script)**  
  [1. Create staging tables (employees_staging and timesheet_staging) to store data from CSV](#1-create-staging-tables-employeesstaging-and-timesheetstaging-to-store-data-from-csv)  
  [2. Cleanse data from staging tables (deduplication and casting process)](#2-cleanse-data-from-staging-tables-deduplication-and-casting-process)  
  [3. Create hourly_branch_salaries as the result table by combining CTEs and subquery](#3-create-hourlybranchsalaries-as-the-result-table-by-combining-ctes-and-subquery)  

**[  V. Solution & Result Logic Detail - Incremental Ingestion Python script](#v-solution--result-logic-detail---incremental-ingestion-python-script)**  
  [1. Prepare environmental requirements to run the script cleanly (Airflow-inspired scripts)](#1-prepare-environmental-requirements-to-run-the-script-cleanly-airflow-inspired-scripts)  
  [2. Connect the script to the SQLAlchemy Postgres engine](#2-connect-the-script-to-the-sqlalchemy-postgres-engine)  
  [3. Create the incremental ingestion logic using Pandas](#3-create-the-incremental-ingestion-logic-using-pandas)  
  [4. Update hourly_branch_salaries](#4-update-hourlybranchsalaries)



## I. Project Overview

Imagine you are a CEO of a company. You ask yourself, how much should I pay for each branches, in each months, and in each years, for their wages. You want to know, whether the current payroll scheme (which is a per-month basis) is reasonably cost-effective in terms of cost per hour.

Therefore, you ask help for your lovely data team (data engineer - Ivan) to figure out about the trend in this case. Therefore, project `Hourly Branch Salaries` will begun. Completing your speech, you also give overview of 2 points - **their dataset to use, and the objective(s) of this project**.

(i.e.)

For each branch, salary per hour based on the number of employees that work for that branch each month.

For E.g. assuming Branch A has 5 people working for it in January; the salary for those 5 people in that month is Rp100,000,000, and the total hours for the same 5 employees in that month is 1000 hours. Therefore, the output should be **Rp100,000 per hour**.



### Dataset(s) currently on possesion

You hand down 2 CSVs on your possesion related to the task you've given - **`employees` and `timesheets`.** This table shows the dataset breakdown.

#### **1. Employees**

Data of the `employees`. This will be the **dimension table** for the task.

| No   | Column Name | Datatype | Short Description                                                |
| :--- | :---------- | :------- | :--------------------------------------------------------------- |
| 1    | employee_id | Int64    | Unique ID of employees working in your company                   |
| 2    | branch_id   | Int64    | The branch which the employee currently  assigned at             |
| 3    | salary      | Int64    | Salary of the employee in a monthly basis                        |
| 4    | join_date   | Date     | The join date of the employee                                    |
| 5    | resign_date | Date     | The resign date of the employee (Null if still actively working) |

#### **2. Timesheets**

Data of the `timesheets` of the employees. This will be the **fact table** for the task.

| No   | Column Name  | Datatype | Short Description                                           |
| :--- | :----------- | :------- | :---------------------------------------------------------- |
| 1    | timesheet_id | Int64    | Unique ID of the timesheet date, checkin, and checkout time |
| 2    | employee_id  | Int64    | The employee id. This is the **Foreign Key**.               |
| 3    | date         | Date     | Date of the timesheet - using ol' plain **YYYY-MM-DD**      |
| 4    | checkin      | Time     | The checkin time of the employee in **HH:MM:SS**            |
| 5    | checkout     | Time     | The checkout time of the employee in **HH:MM:SS**           |



### Objective

You ask Ivan to complete these objectives, that would be reported at the next part:

1. **Full-snapshot SQL script [(in this folder)](/full_snapshot_sql)**  
   - Create a schema and load each CSV file to `employees` and `timesheets` tables.
   
   - Write an SQL script that reads from `employees` and `timesheets` tables, transforms, and loads the destination table.
   
2. **Incremental Ingestion Python script (other scripts)**
   - Write a Python or Java code that reads from CSV files, transforms, and loads to the destination table.
   - The code is expected to run daily in incremental mode, meaning that each day it will only read the new data and then appends the result to the destination table.



## II. Assumptions, Limitations, and Edge Cases

After a few hours, Ivan as the data engineer continues forward analyzing the current datasets and objectives. He found some questions to ask, but you said that you would leave the decision to him for the details. Then, Ivan listed down the assumption and limitations as below, along with his reasoning to do so:



1. **Missing Timesheet Data:**

   - **Employees without timesheet data for a given month are still included in the salary calculations.** 

     In real world scenarios, whether the employees fill out the timesheets or not, the monthly payroll will still go on one way or another. 

   - **If timesheet data is missing (i.e., `checkin` or `checkout` is `NULL`), the employee is assumed to have worked 8 hours for that day.**

     This assumption was made as around 2.080 / 39.715 (~5%) of the total data has missing checkin and checkout date. He figured out that if these rows would be dropped, then the insight taken would not represent the data as it should, and data analyst would appreciate she could get the nearest hour number of the null checkin and checkout time.

2. **Salary and Hours Calculation:**

   - The total hours worked by each employee in a month are capped at **8 hours per day**. As in the real-case scenario, most workers would be counted at 8 hours working, and are expected to clockin before the working time (i.e. if the job starts at 9.00AM, then the employees will have checked in at 8.45, and checked out later than 5.00PM )

   - Overlapping timesheets (multiple check-ins or check-outs per day) are handled by taking the **maximum checkout time** and **minimum check-in time** to avoid double-counting hours worked. 

3. **Duplicate data:**

   - Duplicate data will be deduplicated using their PostgreSQL CTID. Last row would be treated as the most update row.

4. **Invalid Timesheet Data:**

   - Timesheets where the `checkin` time is later than the `checkout` time are treated as invalid and counted as 0 hours for that entry, and there won't be minus delta time for the checkin and checkout.

   - Any records with `NULL` values in `checkin` or `checkout` are counted as 8 hours (per Ivan's assumption for missing data).

5. **Employee Salary Calculation:**

   - The salary for each employee is considered for the entire month, regardless of whether the employee worked every day or if they have missing timesheet records.

   - Salary for employees joining mid-month or resigning mid-month is handled based on the available timesheet data for that month.

6. **Branch Assignments:**
   - **No branch changes** within the month are considered. The branch assignment for each employee is taken directly from the `employees` table. It assumes employees do not switch branches during the month.

7. **Pro-Rating Salary for Partial Months:**
   - The query does **not** pro-rate salaries for employees who join or resign mid-month. The entire monthly salary is used for the calculations even if the employee worked only a portion of the month.

8. **Zero Hours Handling:**
   - If no hours are recorded for an employee in a given month, the total hours for that employee are assumed to be 0, and the resulting cost per hour will be calculated as the salary divided by `1` (to avoid division by zero).

9. **Resignation Handling:**

   - Employees who have resigned and whose resignation date is before the current date are included from the calculations for that month for their last month.

     

## III. Solution & Result Overview

The solution and result overview will be broken down into 2 sections: **Full-snapshot SQL script** and **Incremental Ingestion Python script**. 

### a. Full-snapshot SQL script

Tackling this problem, there are few steps to be taken to generate the results:

1. Create staging tables (employees_staging and timesheet_staging) to store data from CSV
2. Cleanse data from staging tables (deduplication and casting process)
3. Create hourly_branch_salaries as the result table by combining CTEs and subquery

### b. Incremental Ingestion Python script

Different with the full-snapshot, below are the steps taken to achieve the objectives:

1. Prepare environmental requirements to run the script cleanly (Airflow-inspired scripts)
2. Connect the script to the SQLAlchemy Postgres engine
3. Create the incremental ingestion logic using Pandas
4. Update hourly_branch_salaries



## IV. Solution & Result Logic Detail - Full-snapshot SQL script

This part would explain the detail of the part III.a with robust and detailed information.

### 1. Create staging tables (employees_staging and timesheet_staging) to store data from CSV

This mimics the **raw layer** behavior. At the ideal and real-world environment, I would propose to save the CSV raw files, stored in a **"bucket"** or cloud storage like **GCP's GCS or AWS S3** in a date-partitioned folders. This would **reserve the raw data on daily basis**, so that any data loss would be able to be recovered fast, while maintaining the cost low compared to using a full PostgreSQL table for the raw layer.



### 2. Cleanse data from staging tables (deduplication and casting process)

This mimics the process from raw layer to clean layer. During this process, I used the ctid property of PostgreSQL to determine the ranking of the row, as by default, we would choose the latest row data like the example below:

| *(RANK)* | employee_id | branch_id | salary        | join_date      | resign_date    |
| -------- | ----------- | --------- | ------------- | -------------- | -------------- |
| 3        | 1000        | 100       | 1.000.000     | 2024-10-21     | 2025-10-20     |
| 2        | 1000        | 99        | 1.500.000     | 2024-10-21     | 2025-10-20     |
| **1**    | **1000**    | **98**    | **2.500.000** | **2024-10-21** | **2025-10-20** |

From 3 rows, **we would pick the data with rank 1**, as it was the **last inserted row in the CSV** with the same PK (employee_id). The other behaviour that would be seen in this step is **datatype casting**, but based on the data, I decided that a datatype casting was not needed as the data was clean enough. The data that passed this rank over process would be passed from *_staging table into the * table.



### 3. Create hourly_branch_salaries as the result table by combining CTEs and subquery

This **mimics the process from clean layer into datamart layer**. During this process, I used few CTEs and subqueries to create the destination table. The logic that I would use is shown as expression below:

> **hourly_branch_salaries = SUM(monthly_employees_salary_in_that_branch) / SUM(total_hours_of_employees_in_that_month)**

To get each element of the equation, below was the step taken:

1. #### Employee hours (total_hours_of_employees_in_that_month)

   Utilizing the **date**, I took the **year and month**. I will count the real working hour based on my **assumption point (1) and point (2)** by using combination of simple extract, sum, join, and group by. Left join was chosen instead of inner join as I would like to include employees that did not input any checkin and checkout into the branch hourly salary count. The join logic also looks for the **active employees only** (not including the resigned employees in the later months).

2. #### Branch salary (SUM(monthly_employees_salary_in_that_branch))

   This is the most tricky part in the SQL. I use the combined **date_range, date_series, employee_active_period, branch_salary** to get every branchs' salaries. The **date_range** and **data_series** **limit the branch salary data space by every month and year available in the `timesheets` table**, and help the indexing process. Then, in the employee_active period, I took the **employee active in that month** (the one not resigning in the current month). In the last CTE, branch_salary, I combined the active employees, sum up and group them based on the branch, month, and year. **I chose not to combine `employees` and `timesheets`** because it would either end up counting the same employee salaries as one, or count them multiplically (if employee A has 4 timesheet, then his salary would be 4x the real value)

3. #### Main query (hourly_branch_salaries)

   **Combine step 1 and 2 to get the base equation, join, and group by them**. Coalesce is used to make sure that edge cases, like divided by 0 are covered in the query.



## V. Solution & Result Logic Detail - Incremental Ingestion Python script

### 1. Prepare environmental requirements to run the script cleanly (Airflow-inspired scripts)

As the scoring of this code challenge  also applies for the scheduler-applicable, I tried to mimic Airflow-like folder structure. Below are the explanation of the folder structure and the reasoning of each.

| No   | Folder/File                   | Short Description                                            | Reasoning                                                    |
| ---- | ----------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1    | config                        | Holds the configuration-related JSON data (i.e. secret.json) that holds the credentials of the databases being used. This mimics the GCP's secret manager. | It would pose a security risk if the credentials were directly injected in the DAG / custom operator. In real-world scenarios, this folder would be in .gitignore. |
| 2    | custom_operator               | Holds the custom operator(s) that is used in Airflow. The custom operator mimics the OOP-based Airflow custom operator. | OOP is used as it is more robust, and able to encapsulate the methods better. |
| 3    | lib                           | Holds the general functions, i.e. secret manager             | Separated folder would make it easier to maintain should there be any updates |
| 4    | temp_files                    | Holds the CSV files. This mimics the GCP's GCS.              | The file should be able to be easily maintained.             |
| 5    | d_1_hourly_branch_salaries.py | The Airflow DAG. "d" means daily, and "1" means it will only be executed once. | Make the file "readable" by just its name                    |



### 2. Connect the script to the SQLAlchemy Postgres engine

Incremental Ingestion **means that traversion to the existing database is essential**. Using SQLAlchemy, I tried to connect the local PostgreSQL to be used by **Pandas**. Pandas is chosen as it is the most versatile python data manipulation library. **It is also able to give a hint how the pipeline would be should it be scaled in the future (i.e using PySpark).**



### 3. Create the incremental ingestion logic using Pandas

By creating a method in the custom_operator to fit incremental ingestion logic, I tried to use Pandas to merge the new_data (in this case the CSVs) with the existing table into a dataframe by using dynamic method to identify the primary key and the non-primary key columns. This method is usable in another logic, and is very useful to do incremental ingestion. There are 2 key concepts looked for in these lines of code:

- **New Records**

  New records are defined as the **records which was not exist in the existing database**. I distinguish this records by the **primary key column**. If the **primary key column was None/Null in the existing database, this means that the data are new**, and are categorized as new records. These new records then will be appended into the database.

- **Updated Records**

  Differing from new records, updated records **refers to same primary key value, but different non-primary key value** (i.e. salary of an employee rises). This means that the **primary key is retained, but is updated using SQL UPDATE logic.**

  

### 4. Update hourly_branch_salaries

In this logic, it is the same as step (3). **But, this step will not be executed if no changes were found in the `employees` and `timesheets` table** to save and manage resources spent in this logic.

