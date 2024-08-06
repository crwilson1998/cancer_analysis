from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local").appName('movies').getOrCreate()

# Read in the CSV file
cancer_data = spark.read.option('header', True) \
                        .option('delimiter', ',') \
                        .option("inferschema", True) \
                        .csv(r"C:/Users/Chris/PycharmProjects/Projects/DA_Projects/movies/cancer_diagnosis_data.csv")

# Register DataFrame as a temporary view
cancer_data.createOrReplaceTempView("cancer")

# Count all rows
count_all_query = """ SELECT COUNT(*) AS count FROM cancer """
count_all_result = spark.sql(count_all_query).collect()[0]['count']

# Male survived
male_survived_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Survival_Status == 'Survived' AND Gender == 'Male' """
male_survived_result = spark.sql(male_survived_query).collect()[0]['count']

# Female survived
female_survived_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Survival_Status == 'Survived' AND Gender == 'Female' """
female_survived_result = spark.sql(female_survived_query).collect()[0]['count']

# Male deceased
male_deceased_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Survival_Status == 'Deceased' AND Gender == 'Male' """
male_deceased_result = spark.sql(male_deceased_query).collect()[0]['count']

# Female deceased
female_deceased_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Survival_Status == 'Deceased' AND Gender == 'Female' """
female_deceased_result = spark.sql(female_deceased_query).collect()[0]['count']

# Male benign
male_benign_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Tumor_Type == 'Benign' AND Gender == 'Male' """
male_benign_result = spark.sql(male_benign_query).collect()[0]['count']

# Female benign
female_benign_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Tumor_Type == 'Benign' AND Gender == 'Female' """
female_benign_result = spark.sql(female_benign_query).collect()[0]['count']

# Male malignant
male_malignant_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Tumor_Type == 'Malignant' AND Gender == 'Male' """
male_malignant_result = spark.sql(male_malignant_query).collect()[0]['count']

# Female malignant
female_malignant_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Tumor_Type == 'Malignant' AND Gender == 'Female' """
female_malignant_result = spark.sql(female_malignant_query).collect()[0]['count']

# Male chemotherapy
male_chemotherapy_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Chemotherapy' AND Gender == 'Male' """
male_chemotherapy_result = spark.sql(male_chemotherapy_query).collect()[0]['count']

# Female chemotherapy
female_chemotherapy_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Chemotherapy' AND Gender == 'Female' """
female_chemotherapy_result = spark.sql(female_chemotherapy_query).collect()[0]['count']

# Male radiation therapy
male_radiation_therapy_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Radiation Therapy' AND Gender == 'Male' """
male_radiation_therapy_result = spark.sql(male_radiation_therapy_query).collect()[0]['count']

# Female radiation therapy
female_radiation_therapy_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Radiation Therapy' AND Gender == 'Female' """
female_radiation_therapy_result = spark.sql(female_radiation_therapy_query).collect()[0]['count']

# Male surgery
male_surgery_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Surgery' AND Gender == 'Male' """
male_surgery_result = spark.sql(male_surgery_query).collect()[0]['count']

# Female surgery
female_surgery_query = """ SELECT COUNT(*) AS count FROM cancer WHERE Treatment == 'Surgery' AND Gender == 'Female' """
female_surgery_result = spark.sql(female_surgery_query).collect()[0]['count']

# Print the results of each query
print(f"Total Rows: {count_all_result}")
print(f"Male Survived: {male_survived_result}")
print(f"Female Survived: {female_survived_result}")
print(f"Male Deceased: {male_deceased_result}")
print(f"Female Deceased: {female_deceased_result}")
print(f"Male Benign: {male_benign_result}")
print(f"Female Benign: {female_benign_result}")
print(f"Male Malignant: {male_malignant_result}")
print(f"Female Malignant: {female_malignant_result}")
print(f"Male Chemotherapy: {male_chemotherapy_result}")
print(f"Female Chemotherapy: {female_chemotherapy_result}")
print(f"Male Radiation Therapy: {male_radiation_therapy_result}")
print(f"Female Radiation Therapy: {female_radiation_therapy_result}")
print(f"Male Surgery: {male_surgery_result}")
print(f"Female Surgery: {female_surgery_result}")

# Define the CTE for Age Groups and Tumor Sizes
groups = """
WITH age_groups AS (
    SELECT Patient_ID,
        CASE
            WHEN Age >= 20 AND Age <= 29 THEN '20-29'
            WHEN Age >= 30 AND Age <= 39 THEN '30-39'
            WHEN Age >= 40 AND Age <= 49 THEN '40-49'
            WHEN Age >= 50 AND Age <= 59 THEN '50-59'
            WHEN Age >= 60 AND Age <= 69 THEN '60-69'
            ELSE '70+'
        END AS Age_Groups
    FROM cancer
),
tumor_sizes_grouped AS (
    SELECT Patient_ID,
        CASE
            WHEN Tumor_Size < 1 THEN 'Less than 1 cm'
            WHEN Tumor_Size >= 1 AND Tumor_Size <= 1.99 THEN '1cm - 1.99cm'
            WHEN Tumor_Size >= 2 AND Tumor_Size <= 2.99 THEN '2cm - 2.99cm'
            WHEN Tumor_Size >= 3 AND Tumor_Size <= 3.99 THEN '3cm - 3.99cm'
            WHEN Tumor_Size >= 4 AND Tumor_Size <= 4.99 THEN '4cm - 4.99cm'
            WHEN Tumor_Size >= 5 AND Tumor_Size <= 5.99 THEN '5cm - 5.99cm'
            WHEN Tumor_Size >= 6 AND Tumor_Size <= 6.99 THEN '6cm - 6.99cm'
            WHEN Tumor_Size >= 7 AND Tumor_Size <= 7.99 THEN '7cm - 7.99cm'
            WHEN Tumor_Size >= 8 AND Tumor_Size <= 8.99 THEN '8cm - 8.99cm'
            ELSE '9cm or greater'
        END AS Tumor_Sizes
    FROM cancer
)
"""

# Define the query for the best treatment for benign tumors
best_treatment_for_benign = """
SELECT c.Tumor_Type, c.Treatment, ag.Age_Groups,
       ROUND((COUNT(CASE WHEN c.Survival_Status = 'Survived' THEN 1 END) / COUNT(*)) * 100.0, 2) AS Survival_Chance
FROM cancer c
    INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
WHERE c.Tumor_Type = 'Benign'
GROUP BY c.Tumor_Type, c.Treatment, ag.Age_Groups
ORDER BY c.Treatment, Survival_Chance DESC
"""

# Define the query for the best treatment for malignant tumors
best_treatment_for_malignant = """
SELECT c.Treatment, ag.Age_Groups,
       ROUND((COUNT(CASE WHEN c.Survival_Status = 'Survived' THEN 1 END) / COUNT(*)) * 100.0, 2) AS Survival_Chance
FROM cancer c
INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
WHERE c.Tumor_Type = 'Malignant'
GROUP BY c.Tumor_Type, c.Treatment, ag.Age_Groups
ORDER BY c.Treatment, Survival_Chance DESC
"""

# Define the query for male treatment response
male_treatment_response_query = """
SELECT c.Treatment, ag.Age_Groups,
       ROUND((COUNT(CASE WHEN c.Response_to_Treatment IN ('Complete Response', 'Partial Response') THEN 1 END) / COUNT(*)) * 100.0, 2) AS Response_Rate
FROM cancer c
INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
WHERE c.Gender = 'Male'
GROUP BY c.Treatment, ag.Age_Groups
ORDER BY c.Treatment, Response_Rate
"""

# Define the query for female treatment response
female_treatment_response_query = """
SELECT c.Treatment, ag.Age_Groups,
       ROUND((COUNT(CASE WHEN c.Response_to_Treatment IN ('Complete Response', 'Partial Response') THEN 1 END) / COUNT(*)) * 100.0, 2) AS Response_Rate
FROM cancer c
INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
WHERE c.Gender = 'Female'
GROUP BY c.Treatment, ag.Age_Groups
ORDER BY c.Treatment, Response_Rate
"""

male_tumor_detection = """
SELECT ag.Age_Groups, tg.Tumor_Sizes,
       ROUND((COUNT(CASE WHEN c.Biopsy_Result = 'Positive' THEN 1 END) / COUNT(*)) * 100.0, 2) AS Detection_Rate
FROM cancer c
INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
INNER JOIN tumor_sizes_grouped tg ON c.Patient_ID = tg.Patient_ID
WHERE c.Gender = 'Male'
GROUP BY ag.Age_Groups, tg.Tumor_Sizes
HAVING tg.Tumor_Sizes = '1cm - 1.99cm'
ORDER BY ag.Age_Groups
"""

female_tumor_detection = """
SELECT ag.Age_Groups, tg.Tumor_Sizes,
       ROUND((COUNT(CASE WHEN c.Biopsy_Result = 'Positive' THEN 1 END) / COUNT(*)) * 100.0, 2) AS Detection_Rate
FROM cancer c
INNER JOIN age_groups ag ON c.Patient_ID = ag.Patient_ID
INNER JOIN tumor_sizes_grouped tg ON c.Patient_ID = tg.Patient_ID
WHERE c.Gender = 'Female'
GROUP BY ag.Age_Groups, tg.Tumor_Sizes
HAVING tg.Tumor_Sizes = '1cm - 1.99cm'
ORDER BY ag.Age_Groups
"""

# Combine the CTE and the main queries
complete_query_benign = f"{groups} {best_treatment_for_benign}"
complete_query_malignant = f"{groups} {best_treatment_for_malignant}"
complete_query_male_response = f"{groups} {male_treatment_response_query}"
complete_query_female_response = f"{groups} {female_treatment_response_query}"
complete_query_male_detection = f"{groups} {male_tumor_detection}"
complete_query_female_detection = f"{groups} {female_tumor_detection}"

# Execute the combined queries
result_df_benign = spark.sql(complete_query_benign)
result_df_malignant = spark.sql(complete_query_malignant)
result_df_male_response = spark.sql(complete_query_male_response)
result_df_female_response = spark.sql(complete_query_female_response)
result_df_male_detection = spark.sql(complete_query_male_detection)
result_df_female_detection = spark.sql(complete_query_female_detection)

# Show the results
# result_df_benign.show(truncate=False)
# result_df_malignant.show(truncate=False)
# result_df_male_response.show(truncate=False)
# result_df_female_response.show(truncate=False)
# result_df_male_detection.show(truncate=False)
# result_df_female_detection.show(truncate=False)

# End spark session
spark.stop()