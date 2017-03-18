connectionDetails <- 
  createConnectionDetails(dbms = Sys.getenv("cdmDbms"), 
                          server = Sys.getenv("cdmServer"), 
                          port = Sys.getenv("cdmServerPort")) # edit as needed for your DBMS
cdmDatabaseSchema <- Sys.getenv("cdmDatabaseSchema") # the schema with your CDM data
resultsDatabaseSchema <- Sys.getenv("resultsDatabaseSchema") # the schema with your Cohort table
cohortDefinitionId <- Sys.getenv("phenotypeCohortId") # set your CohortDefinitionId

connection <- connect(connectionDetails)
sql <- renderSql(sql = "select cdm_source_name from @cdmDatabaseSchema.cdm_source", 
                 cdmDatabaseSchema = cdmDatabaseSchema)$sql
cdmSourceName <- querySql(connection, sql)
dbDisconnect(connection)

stratifyIncidence(connectionDetails = connectionDetails,
                  cdmSourceName = cdmSourceName,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  resultsDatabaseSchema = resultsDatabaseSchema,
                  cohortDefinitionId = cohortDefinitionId,
                  cohortTableName = "cohort", # change this to the name of your cohort table
                  yearStart = "", # blank = use minimum available year, otherwise specify
                  yearEnd = "") # blank = use maximum available year otherwise specify


aggregateIncidence(connectionDetails = connectionDetails,
                   cdmSourceName = cdmSourceName$CDM_SOURCE_NAME,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   resultsDatabaseSchema = resultsDatabaseSchema,
                   cohortDefinitionId = cohortDefinitionId)

runDrugOutcomeSummary(connectionDetails = connectionDetails, 
                      cdmSourceName = cdmSourceName, 
                      cdmDatabaseSchema = cdmDatabaseSchema, 
                      resultsDatabaseSchema = resultsDatabaseSchema, 
                      cohortDefinitionId = cohortDefinitionId)
