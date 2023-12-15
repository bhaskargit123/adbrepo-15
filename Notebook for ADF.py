# Databricks notebook source
# DBTITLE 1,Declaring Schema
empSchema = "EmployeeNO INTEGER, ENAME STRING, JOB STRING, MGR INTEGER, HIREDATE DATE, SAL DOUBLE, COMM DOUBLE, DepartmentNO INTEGER"

# COMMAND ----------

# DBTITLE 1,Creating employeedf dataframe
employeeDef = spark.read.format("csv").options( header=True, sep=",").schema(empSchema).load("/mnt/adls/input/employee.csv")

# COMMAND ----------

# DBTITLE 1,Creating vemployee View
employeeDef.createOrReplaceTempView("vemployee")

# COMMAND ----------

# DBTITLE 1,Getting Top 3rd Highest Salary
dfhighest = spark.sql(
  """
  SELECT * FROM (
  SELECT *, dense_rank() OVER (ORDER BY SAL DESC) AS Rnk
  FROM vemployee
)T where Rnk=3
  """
)

# COMMAND ----------

# DBTITLE 1,Loading Data into ADLS
dfhighest.write.format("csv").option("header", "true").save("/mnt/adls/output/employee")
