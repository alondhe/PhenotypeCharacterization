connectionDetails <- 
  createConnectionDetails(dbms = Sys.getenv("cdmDbms"), 
                          server = Sys.getenv("cdmServer"), 
                          port = Sys.getenv("cdmServerPort")) # edit as needed for your DBMS
cdmDatabaseSchema <- Sys.getenv("cdmDatabaseSchema") # the schema with your CDM data
resultsDatabaseSchema <- Sys.getenv("resultsDatabaseSchema") # the schema with your Cohort table
cohortDefinitionId <- Sys.getenv("phenotypeCohortId") # set your CohortDefinitionId


characterize(connectionDetails = connectionDetails, 
             cdmDatabaseSchema = cdmDatabaseSchema, 
             resultsDatabaseSchema = resultsDatabaseSchema,
             cohortDefinitionId = cohortDefinitionId)  

