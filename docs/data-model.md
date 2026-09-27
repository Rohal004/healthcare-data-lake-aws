# Healthcare Data Model

## Patient Dataset

| Column | Description |
|---|---|
| patient_id | Unique identifier for each patient record |
| name | Patient name |
| age | Patient age |
| gender | Patient gender |
| admission_date | Hospital admission date |
| diagnosis | Recorded diagnosis |
| hospital_id | Identifier of the source hospital |

## Data Lake Layers

### Raw

Contains the original hospital CSV files before validation.

### Validated

Contains records that pass all configured data quality rules.

### Rejected

Contains records that fail one or more data quality rules and are
retained for auditing and potential reprocessing.

## Glue Catalog

Database: `healthcare_db`

Tables:

- `raw`
- `validated_validated`
- `rejected_rejected`