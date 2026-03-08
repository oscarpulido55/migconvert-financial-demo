WITH EmployeeJobHistory AS (
    -- Section 1: Combine Current Job with Historical Jobs
    -- We use UNION ALL to merge two datasets: the employee's current role and their past roles.

    -- Get the current job for each employee from the 'employees' table
    SELECT
        employee_id,
        job_id,
        department_id,
        start_date,
        TO_DATE(NULL) AS end_date, -- Current jobs have no end date
        'Current' AS job_status
    FROM
        hr.employees
    WHERE
        start_date IS NOT NULL

    UNION ALL

    -- Get the historical jobs for each employee from the 'job_history' table
    SELECT
        employee_id,
        job_id,
        department_id,
        start_date,
        end_date,
        'Past' AS job_status
    FROM
        hr.job_history
)
-- Main Query: Join all information together
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    j.job_title,
    d.department_name,
    ejh.start_date,
    ejh.end_date,
    ejh.job_status,
    e.salary,
    -- Section 2: Window Function for Salary Analysis
    -- Calculate the average salary for the employee's department and compare.
    -- PARTITION BY d.department_id ensures the average is calculated for each department separately.
    ROUND(AVG(e.salary) OVER (PARTITION BY d.department_name), 2) AS avg_department_salary,
    CASE
        WHEN e.salary > AVG(e.salary) OVER (PARTITION BY d.department_name) THEN 'Above Average'
        WHEN e.salary < AVG(e.salary) OVER (PARTITION BY d.department_name) THEN 'Below Average'
        ELSE 'Average'
    END AS salary_comparison,
    -- Section 3: Manager and Location Details
    -- Self-join on the employees table to get the manager's name and join other tables for location.
    m.first_name || ' ' || m.last_name AS manager_name,
    l.street_address || ', ' || l.city || ', ' || l.state_province || ' ' || l.postal_code AS full_location,
    c.country_name,
    r.region_name
FROM
    hr.employees e
LEFT JOIN
    EmployeeJobHistory ejh ON e.employee_id = ejh.employee_id
LEFT JOIN
    hr.jobs j ON ejh.job_id = j.job_id
LEFT JOIN
    hr.departments d ON ejh.department_id = d.department_id
LEFT JOIN
    hr.employees m ON e.manager_id = m.employee_id
LEFT JOIN
    hr.locations l ON d.location_id = l.location_id
LEFT JOIN
    hr.countries c ON l.country_id = c.country_id
LEFT JOIN
    hr.regions r ON c.region_id = r.region_id
ORDER BY
    e.employee_id,
    ejh.start_date DESC;
