# STEDI Step Trainer Project

This project implements an AWS Glue lakehouse pipeline for STEDI's Human Balance Analytics data. The pipeline ingests, transforms, and curates data from IoT and mobile app sources using AWS Glue, S3, and Athena.

## 📂 Project Structure

```
stedi-step-trainer-project/
├── glue_jobs/
│   ├── accelerometer_landing_to_trusted.py
│   ├── customer_landing_to_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── machine_learning_curated.py
│   └── step_trainer_trusted.py
├── screenshots/
│   ├── landing/
│   ├── trusted/
│   └── curated/
├── sql/
│   ├── landing/
│   │   ├── accelerometer_landing.sql
│   │   ├── customer_landing.sql
│   │   └── step_trainer_landing.sql
│   ├── trusted/
│   │   ├── accelerometer_trusted.sql
│   │   ├── customer_trusted.sql
│   │   └── step_trainer_trusted.sql
│   └── curated/
│       ├── customer_curated.sql
│       └── machine_learning_curated.sql
└── README.md

```


## 🚀 Glue Jobs

- **customer_landing_to_trusted.py**: Filters customers who consented to share data (`sharewithresearchasofdate IS NOT NULL`).
- **accelerometer_landing_to_trusted.py**: Joins accelerometer data with trusted customers (by email).
- **step_trainer_trusted.py**: Joins step trainer data with curated customers (by serial number).
- **customer_trusted_to_curated.py**: Joins trusted customers with trusted accelerometer data.
- **machine_learning_curated.py**: Aggregates step trainer + accelerometer data by timestamp for machine learning use.

## 📊 Athena Queries & Row Counts

- `customer_landing`: 956 rows
- `accelerometer_landing`: 81273 rows
- `step_trainer_landing`: 28680 rows

- `customer_trusted`: 482 rows
- `accelerometer_trusted`: 40981 rows
- `step_trainer_trusted`: 14460 rows

- `customer_curated`: 482 rows
- `machine_learning_curated`: 43681 rows

## 💡 Notes

- All Glue jobs dynamically update the Data Catalog schema.
- SQL scripts provided create external tables on S3 JSON data.
- Screenshots show query results in Athena for validation.

---

## 📌 How to Run

1. Upload the scripts to AWS Glue.
2. Set up the S3 directories:
   - `s3://your-bucket/customer/landing/`
   - `s3://your-bucket/accelerometer/landing/`
   - `s3://your-bucket/step_trainer/landing/`
3. Run Glue jobs in sequence:
   - Landing → Trusted → Curated
4. Query results using Athena.

---

## 🔗 Author

Jake Hotchkiss  
[GitHub: TheDataDawg](https://github.com/TheDataDawg)