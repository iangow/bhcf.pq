# Build bhcf_types from a public Google Sheet.
#
# Usage:
#   source("data-raw/bhcf_types_from_sheet.R")
#   build_bhcf_types_from_sheet(
#     sheet_url = "https://docs.google.com/spreadsheets/d/1xUcES70vaqi9Gtn9VjSwRS0O23rOk53StzpmO_rGikk"
#   )

build_bhcf_types_from_sheet <- function(
    sheet_url,
    sheet = "types",
    out_data_file = "data/bhcf_types.rda",
    out_extdata_file = "inst/extdata/bhcf_types.parquet") {

  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required to read the sheet.", call. = FALSE)
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write parquet output.", call. = FALSE)
  }

  googlesheets4::gs4_deauth()

  lookup <- googlesheets4::read_sheet(
    sheet_url,
    sheet = sheet,
    col_types = "cc"
  )

  lookup <- tibble::as_tibble(lookup) |>
    dplyr::rename_with(tolower) |>
    dplyr::transmute(
      item = as.character(.data$item),
      data_type = tolower(as.character(.data$data_type))
    ) |>
    dplyr::filter(!is.na(.data$item), nzchar(.data$item))

  allowed <- c("text", "integer", "float", "date")
  bad <- setdiff(unique(stats::na.omit(lookup$data_type)), allowed)
  if (length(bad) > 0L) {
    stop(
      "Unexpected `data_type` values in lookup: ",
      paste(bad, collapse = ", "),
      call. = FALSE
    )
  }

  lookup <- dplyr::distinct(lookup, .data$item, .keep_all = TRUE)

  dir.create(dirname(out_data_file), recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(out_extdata_file), recursive = TRUE, showWarnings = FALSE)

  bhcf_types <- lookup
  save(bhcf_types, file = out_data_file, compress = "bzip2")
  arrow::write_parquet(lookup, out_extdata_file)

  invisible(lookup)
}
