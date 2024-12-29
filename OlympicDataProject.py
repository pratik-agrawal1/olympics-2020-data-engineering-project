#This Python file has to be run in Azure Databricks in a notebook. Before running the file, make sure to load the data provided within the /data folder into the Azure Storage Account
# using Azure Data Factory.


#Configure the app created from app registry in Azure to be able to talk to Azure Storage Account
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "", #provide the client ID from the app registry in Azure
           "fs.azure.account.oauth2.client.secret": "", #provide the secret key from the app created in app registry in Azure
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<provide-tenant-ID>/oauth2/token"} #provide the tenant ID from the app registry in Azure


#Mounting the storage container to be used for reading and writing the data
dbutils.fs.mount(
    source = "abfss://tokyo-olympic-data@olympicsdata2020.dfs.core.windows.net",
    mount_point = "/mnt/tokyo-olympic-data",
    extra_configs = configs) 


#Checking whether the Azure Storage Account container has been successfuly mounted or not
%fs
ls  "/mnt/tokyo-olympic-data/raw-data"


#Reading the raw files from the mounted Azure Storage Account
athletes_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyo-olympic-data/raw-data/athletes.csv")
coaches_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyo-olympic-data/raw-data/coaches.csv")
entriesgender_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyo-olympic-data/raw-data/entriesgender.csv")
medals_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyo-olympic-data/raw-data/medals.csv")
teams_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyo-olympic-data/raw-data/teams.csv")


#Dropping unnecessary columns
athletes_df = athletes_df.drop("_c3","_c4")
coaches_df = coaches_df.drop("_c4")
entriesgender_df = entriesgender_df.drop("_c4")
teams_df = teams_df.drop("_c4")


#Sample Data Analysis query using Apache Spark
#Calculate the average number of entries by gender for each discipline
entriesgender_df.selectExpr(
    "Discipline",
    "round(Female/Total,2) as Female_Avg",
    "round(Male/Total,2) as Male_Avg"
).show(5)



#Writing the cleaned and transformed data back into /transformed-data folder within the container created in Azure Storage account. These files will be then used to create table in 
# Azure Synapse Analytics to query the data to build business insights. Can be integrated with a reporting tool to create meaningful visualizations.
athletes_df.write.mode("overwrite").format("csv").option("header", "true").save('/mnt/tokyo-olympic-data/transformed-data/athletes.csv')
coaches_df.write.mode("overwrite").format("csv").option("header", "true").save('/mnt/tokyo-olympic-data/transformed-data/coaches.csv')
entriesgender_df.write.mode("overwrite").format("csv").option("header", "true").save('/mnt/tokyo-olympic-data/transformed-data/entriesgender.csv')
medals_df.write.mode("overwrite").format("csv").option("header", "true").save('/mnt/tokyo-olympic-data/transformed-data/medals.csv')
teams_df.write.mode("overwrite").format("csv").option("header", "true").save('/mnt/tokyo-olympic-data/transformed-data/teams.csv')
