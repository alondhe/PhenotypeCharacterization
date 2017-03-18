.onLoad <- function(libname, pkgname)
{
  if (!dir.exists("output"))
  {
    dir.create("output")
  }
}

#' @title
#' stratifyIncidence
# @author Ajit Londhe, Christopher Knoll

#' Run Stratify Characterization Query for a Cohort
#'
#' @details
#' Generates table of cohort incidence, stratified by age and year
#'
#' @return
#' none
#'
#' @param connectionDetails      The connection details for your CDM
#' @param cdmSourceName          The name of your CDM source
#' @param cdmDatabaseSchema      The fully qualified schema name of your CDM
#' @param resultsDatabaseSchema  The fully qualified schema name of where your cohort-like table is found
#' @param cohortDefinitionId     The id of the cohort
#' @param cohortTableName        The name of your cohort table; default is "cohort"
#' @param yearStart              If necessary, the earliest year you want to run this on; default is take earliest available
#' @param yearEnd                If necessary, the latest year you want to run this on; default is take latest available
#' 
#' @export
stratifyIncidence <- function(connectionDetails, 
                         cdmSourceName,
                         cdmDatabaseSchema, 
                         resultsDatabaseSchema, 
                         cohortDefinitionId,
                         cohortTableName = "cohort",
                         yearStart = "",
                         yearEnd = "",
                         sqlOnly = FALSE)
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "stratified.sql", 
                                    packageName = "PhenotypeCharacterization", 
                                    dbms = connectionDetails$dbms, 
                                    yearStart = yearStart,
                                    yearEnd = yearEnd,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    resultsDatabaseSchema = resultsDatabaseSchema,
                                    cohortDefinitionId = cohortDefinitionId,
                                    cohortTableName = cohortTableName)
  if (sqlOnly == TRUE)
  {
    writeSql(sql,"stratified.sql");
    return();
  }
  connection <- connect(connectionDetails)
  result <- querySql(connection = connection, sql = sql)
  dbDisconnect(connection)
  write.csv(x = result, 
            file = paste0("output/", paste(cohortDefinitionId, cdmSourceName, "stratified.csv", sep = "_", collapse = "")), 
            na = "0", row.names = FALSE)
}

#' @title
#' aggregateIncidence
# @author Ajit Londhe, Christopher Knoll

#' Run Aggregation of Incidence Query
#'
#' @details
#' Generates a cohort's overall incidence in person-time for a CDM
#'
#' @return
#' none
#' 
#' @param connectionDetails      The connection details for your CDM
#' @param cdmSourceName          The name of your CDM source
#' @param cdmDatabaseSchema      The fully qualified schema name of your CDM
#' @param resultsDatabaseSchema  The fully qualified schema name of where your cohort-like table is found
#' @param cohortDefinitionId     The id of the cohort
#' @param cohortTableName        The name of your cohort table; default is "cohort"
#' @param yearStart              If necessary, the earliest year you want to run this on; default is take earliest available
#' @param yearEnd                If necessary, the latest year you want to run this on; default is take latest available
#' 
#' @export
aggregateIncidence <- function(connectionDetails, 
                                    cdmSourceName, 
                                    cdmDatabaseSchema, 
                                    resultsDatabaseSchema, 
                                    cohortDefinitionId,
                                cohortTableName = "cohort",
                                yearStart = "",
                                yearEnd = "",
                                sqlOnly = FALSE)
{
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "aggregated.sql", 
                                           packageName = "PhenotypeCharacterization", 
                                           dbms = connectionDetails$dbms, 
                                           yearStart = yearStart,
                                           yearEnd = yearEnd,
                                           cdmSourceName = cdmSourceName,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           cohortDefinitionId = cohortDefinitionId,
                                           cohortTableName = cohortTableName)
  if (sqlOnly == TRUE)
  {
    writeSql(sql,"aggregated.sql");
    return();
  }
  
  connection <- connect(connectionDetails)
  result <- querySql(connection = connection, sql = sql)
  dbDisconnect(connection)
  write.csv(x = result, 
            file = paste0("output/", paste(cohortDefinitionId, cdmSourceName, "aggregated.csv", sep = "_", collapse = "")), 
            na = "0", row.names = FALSE)
}

#' @title
#' plotIncidenceTrellis
# @author Ajit Londhe, Christopher Knoll

#' Plot prevalence stratified by age, gender
#'
#' @details
#' Plot prevalence stratified by age, gender in trellis
#'
#' @return
#' none
#' 
#' @param result      The data frame with your stratified incidence data
#'
#' @export
plotIncidenceTrellis <- function(result)
{
  #result <- read.csv("phenotypeDemo.csv")
  result$INCIDENCE <- result$INCIDENCE*1000
  
  deciles <- sort(unique(result$AGE_DECILE))
  plots <- lapply(deciles, function(decile) {
    plot_ly(data = result[result$AGE_DECILE == decile,],
            x = ~INDEX_YEAR,
            y = ~INCIDENCE,
            color = ~GENDER,
            colors = "Dark2") %>%
      add_lines(name = "GENDER") %>%
      layout(
        title = "Patients with Condition Stratified by Age, Gender",
        xaxis = list(title = "Year", range = c(min(result$INDEX_YEAR), max(result$INDEX_YEAR))),
        yaxis = list(title = "Incidence per 1000 patients", rangemode = "nonnegative"))
  })
  subplot(plots, shareY = TRUE, titleX = FALSE, titleY = TRUE)
}

#' @title
#' runDrugOutcomeSummary
# @author Ajit Londhe, Patrick Ryan

#' Run Drug Outcome Summary
#'
#' @details
#' Runs Drug Outcome Summary query
#'
#' @return
#' none
#'
#' @param connectionDetails      The connection details for your CDM
#' @param cdmDatabaseSchema      The fully qualified schema name of your CDM
#' @param resultsDatabaseSchema  The fully qualified schema name of where your cohort-like table is found
#' @param cohortDefinitionId     The id of the cohort
#' @param cohortTableName        The name of your cohort table; default is "cohort"
#' 
#' @export
runDrugOutcomeSummary <- function(connectionDetails, 
                                  cdmDatabaseSchema, 
                                  resultsDatabaseSchema, 
                                  cohortDefinitionId,
                                  cohortTableName = "cohort",
                                  sqlOnly = FALSE)
{
  ## This is a hack for optimizing table distributions in PDW and Redshift
  ctasHashSubjectId <- ctasHashPersonId <- ctasTempPdw <- ""
  if (connectionDetails$dbms == "pdw")
  {
    ctasHashSubjectId <- "with (location=user_db, distribution=hash(subject_id))"
    ctasHashPersonId <- "with (location=user_db, distribution=hash(person_id))"
    ctasTempPdw <- "with (location=user_db, distribution=replicate)"
  }
  if (connectionDetails$dbms == "redshift")
  {
    ctasHashSubjectId <- "distkey(subject_id)"
    ctasHashPersonId <- "distkey(person_id)"
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "drug_outcome_summary.sql", 
                                           packageName = "PhenotypeCharacterization", 
                                           dbms = connectionDetails$dbms,
                                           cdmSourceName = cdmSourceName,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           cohortDefinitionId = cohortDefinitionId,
                                           cohortTableName = cohortTableName)
  sql <- str_replace_all(string = sql, pattern = "-- HACK ", replacement = "")
  sql <- renderSql(sql = sql, 
                   ctasHashSubjectId = ctasHashSubjectId,
                   ctasHashPersonId = ctasHashPersonId,
                   ctasTempPdw = ctasTempPdw)$sql
  if (sqlOnly == TRUE)
  {
    writeSql(sql,"drug_outcome_summary.sql");
    return();
  }
  
  connection <- connect(connectionDetails)
  executeSql(connection = connection, sql)
  result <- querySql(connection = connection, 
                     sql = "select * from #hackathon_cohort_outcome_summary")
  dbDisconnect(connection)
  write.csv(x = result, file = "output/drug_outcome_summary.csv", na = "0", row.names = FALSE)
  write.csv(x = result, 
            file = paste0("output/", paste(cohortDefinitionId, cdmSourceName, 
                                           "drug_outcome_summary.csv", sep = "_", collapse = "")), 
            na = "0", row.names = FALSE)
}
                               