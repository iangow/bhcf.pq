#' BHCF item master table
#'
#' A reference table containing one row per BHCF data item known to
#' \code{bhcf.pq}.
#'
#' The table includes item identifiers, split mnemonic/item-code components,
#' optional human-readable names, and canonical Arrow-like data types.
#'
#' @format A tibble with columns:
#' \describe{
#'   \item{item}{BHCF item identifier (e.g., \code{"BHSP2170"}).}
#'   \item{mnemonic}{Four-character mnemonic prefix (e.g., \code{"BHSP"}).}
#'   \item{item_code}{Item code suffix (e.g., \code{"2170"}).}
#'   \item{item_name}{Human-readable item label, if available.}
#'   \item{data_type}{Canonical data type label (e.g., \code{"Float64"},
#'     \code{"Int32"}, \code{"Utf8"}, \code{"Date32"}, \code{"Boolean"}).}
#' }
#'
#' @details
#' This dataset is built in \code{data-raw/bhcf_items.R}, typically from
#' \code{bhcf_types} (or the same lookup source used to generate it).
#'
#' @examples
#' bhcf_items
#' dplyr::filter(bhcf_items, mnemonic == "BHSP")
"bhcf_items"
