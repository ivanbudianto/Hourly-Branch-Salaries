
DROP TABLE IF EXISTS hourly_branch_salaries;
CREATE TABLE hourly_branch_salaries as (
WITH employee_hours AS (
    SELECT 
        e.employee_id,
        e.branch_id,
        EXTRACT(YEAR FROM t.date) AS year,
        EXTRACT(MONTH FROM t.date) AS month,
        SUM(
            CASE 
                WHEN t.checkin IS NULL OR t.checkout IS NULL THEN 8  -- Default to 8 hours if checkin/checkout is NULL
                WHEN DATE_TRUNC('minute', t.checkin) >= DATE_TRUNC('minute', t.checkout) THEN 0  -- Invalid data handling
                ELSE EXTRACT(EPOCH FROM (DATE_TRUNC('minute', t.checkout) - DATE_TRUNC('minute', t.checkin))) / 3600  -- Convert to hours
            END
        ) AS total_hours_per_month
    FROM {employees_table} e
    LEFT JOIN {timesheets_table} t 
        ON e.employee_id = t.employee_id
        AND t.date >= e.join_date
        AND (e.resign_date IS NULL OR t.date < e.resign_date)
    GROUP BY e.employee_id, e.branch_id, year, month
),
date_range AS (
    SELECT 
        date_trunc('month', MIN(date)) AS start_date,
        date_trunc('month', MAX(date)) AS end_date
    FROM {timesheets_table}
),
date_series AS (
    SELECT 
        generate_series(
            (SELECT start_date FROM date_range),
            (SELECT end_date FROM date_range),
            '1 month'::interval
        )::date AS month_year
),
employee_active_period AS (
    SELECT 
        employee_id,
        branch_id,
        salary,
        generate_series(
            date_trunc('month', join_date), 
            COALESCE(date_trunc('month', resign_date), (SELECT end_date FROM date_range)),
            '1 month'::interval
        )::date AS active_month
    FROM 
        {employees_table}
),
branch_salary as (
	SELECT 
	    EXTRACT(YEAR FROM d.month_year) AS year,
	    EXTRACT(MONTH FROM d.month_year) AS month,
	    e.branch_id,
	    COALESCE(SUM(e.salary), 0) AS total_salary_per_month
	FROM 
	    date_series d
	LEFT JOIN 
	    employee_active_period e
	ON 
	    d.month_year = e.active_month
	GROUP BY 
	    year, month, e.branch_id
    )
SELECT 
    eh.branch_id,
    eh.year,
    eh.month,
    bs.total_salary_per_month,
    COALESCE(SUM(eh.total_hours_per_month), 0) AS total_hours_per_branch,
    bs.total_salary_per_month / COALESCE(NULLIF(SUM(eh.total_hours_per_month), 0), 1) AS cost_per_hour
FROM employee_hours eh
JOIN branch_salary bs 
    ON eh.branch_id = bs.branch_id 
    AND eh.year = bs.year 
    AND eh.month = bs.month
GROUP BY eh.branch_id, eh.year, eh.month, bs.total_salary_per_month
ORDER BY eh.branch_id, eh.year, eh.month
);
