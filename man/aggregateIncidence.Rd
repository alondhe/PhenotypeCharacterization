% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/main.R
\name{aggregateIncidence}
\alias{aggregateIncidence}
\title{aggregateIncidence
Run Aggregation of Incidence Query}
\usage{
aggregateIncidence(connectionDetails, cdmSourceName, cdmDatabaseSchema,
  resultsDatabaseSchema, cohortDefinitionId, cohortTableName = "cohort",
  yearStart = "", yearEnd = "")
}
\arguments{
\item{connectionDetails}{The connection details for your CDM}

\item{cdmSourceName}{The name of your CDM source}

\item{cdmDatabaseSchema}{The fully qualified schema name of your CDM}

\item{resultsDatabaseSchema}{The fully qualified schema name of where your cohort-like table is found}

\item{cohortDefinitionId}{The id of the cohort}

\item{cohortTableName}{The name of your cohort table; default is "cohort"}

\item{yearStart}{If necessary, the earliest year you want to run this on; default is take earliest available}

\item{yearEnd}{If necessary, the latest year you want to run this on; default is take latest available}
}
\value{
none
}
\details{
Generates a cohort's overall incidence in person-time for a CDM
}

