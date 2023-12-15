# Databricks notebook source
dbutils.fs.mkdirs("/mnt/adls")

# COMMAND ----------

storageAccountName = "bhaskaradlsgen2"
storageAccountAccessKey = "Iy0PZARd2FGJFK184uee7TIBDWSbcFk0jkoGXMkgZWlpPWHj//eFhS41fSNZ1F6Q0rui8iUSaaTe+AStlNCu/Q=="
storageBlobContainerName = "datafiles"

# COMMAND ----------

dbutils.fs.mount(
	source = "wasbs://{}@{}.blob.core.windows.net".format(storageBlobContainerName, storageAccountName),
  mount_point = "/mnt/adls",
  extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net' : storageAccountAccessKey}
)
