#' BHCF type lookup table
#'
#' Lookup used to map BHCF item codes to parser data types.
#' Expected values in `data_type` are `text`, `integer`, `float`, and `date`.
#'
#' This object can be regenerated from the Google Sheet source using
#' `data-raw/bhcf_types_from_sheet.R`.
#'
#' @format A tibble with columns:
#' \describe{
#'   \item{item}{BHCF item code as character.}
#'   \item{data_type}{Type label used by the BHCF parser.}
#' }
"bhcf_types"
