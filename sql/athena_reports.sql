-- ============================================================
-- Healthcare Data Lake on AWS
-- Amazon Athena Analytics Queries
-- ============================================================


-- ------------------------------------------------------------
-- Report 1: Patient Demographics by Gender
-- ------------------------------------------------------------

SELECT
    gender,
    COUNT(*) AS patient_count,
    ROUND(AVG(age), 1) AS avg_age
FROM healthcare_db.validated_validated
GROUP BY gender
ORDER BY patient_count DESC;


-- ------------------------------------------------------------
-- Report 2: Top Diagnoses
-- ------------------------------------------------------------

SELECT
    diagnosis,
    COUNT(*) AS diagnosis_count
FROM healthcare_db.validated_validated
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 10;


-- ------------------------------------------------------------
-- Report 3: Admissions per Hospital
-- ------------------------------------------------------------

SELECT
    hospital_id,
    COUNT(*) AS total_admissions
FROM healthcare_db.validated_validated
GROUP BY hospital_id
ORDER BY total_admissions DESC;


-- ------------------------------------------------------------
-- Report 4: Patient Age Distribution
-- ------------------------------------------------------------

SELECT
    CASE
        WHEN age < 18 THEN '0-17'
        WHEN age BETWEEN 18 AND 35 THEN '18-35'
        WHEN age BETWEEN 36 AND 55 THEN '36-55'
        ELSE '56+'
    END AS age_group,
    COUNT(*) AS patients
FROM healthcare_db.validated_validated
GROUP BY 1
ORDER BY 1;


-- ------------------------------------------------------------
-- Report 5: Rejected Patient Records Audit
-- ------------------------------------------------------------

SELECT
    patient_id,
    name,
    age,
    gender,
    admission_date,
    diagnosis,
    hospital_id
FROM healthcare_db.rejected_rejected
ORDER BY patient_id;