.onLoad <- function(libname, pkgname)
{
}

# @file main
# @author Ajit Londhe

#' Run Phenotype Characterization Query
#'
#' @details
#' Generates table of cohort measures, stratified
#'
#' @return
#' none
#'
#' @export
characterize <- function(connectionDetails, 
                         cdmDatabaseSchema, 
                         resultsDatabaseSchema, 
                         cohortDefinitionId)
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "stratified.sql", 
                                    packageName = "PhenotypeCharacterization", 
                                    dbms = connectionDetails$dbms, 
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    resultsDatabaseSchema = resultsDatabaseSchema,
                                    cohortDefinitionId = cohortDefinitionId)
  
  connection <- connect(connectionDetails)
  result <- querySql(connection = connection, sql = sql)
  write.csv(x = result, file = "phenotypeDemo.csv", na = "0", row.names = FALSE)
}
                               