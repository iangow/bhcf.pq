#' Get selected BHCF items as wide data in one step
#'
#' Convenience wrapper that scans long BHCF parquet files of one value type and
#' pivots selected items to wide format.
#'
#' @param conn A valid DuckDB connection.
#' @param items Character vector of BHCF item codes to return as columns.
#' @param type One of `"float"`, `"int"`, `"text"`, `"date"`, or `"bool"`.
#' @param data_dir Optional parent directory for Parquet output.
#' @param schema Schema subdirectory. Default is `"bhcf"`.
#' @param id_cols Identifier columns for the wide output. Default is
#'   `c("RSSD9001", "date")`.
#' @param values_fn Aggregate function used during pivot (default `"first"`).
#'
#' @return A DuckDB-backed lazy `tbl` in wide format.
#' @export
bhcf_get_items <- function(conn,
                           items,
                           type = "float",
                           data_dir = NULL,
                           schema = "bhcf",
                           id_cols = c("RSSD9001", "date"),
                           values_fn = "first") {

  stopifnot(
    DBI::dbIsValid(conn),
    is.character(items), length(items) >= 1L,
    is.character(type), length(type) == 1L
  )

  type <- tolower(type)
  allowed <- c("float", "int", "text", "date", "bool")
  if (!type %in% allowed) {
    stop("`type` must be one of: ", paste(allowed, collapse = ", "), call. = FALSE)
  }

  long_tbl <- bhcf_scan_pqs(
    conn = conn,
    pq_file = sprintf("bhcf_%s_*.parquet", type),
    data_dir = data_dir,
    schema = schema,
    keep_filename = FALSE
  )

  bhcf_pivot(
    data = long_tbl,
    id_cols = id_cols,
    names_from = "item",
    values_from = "value",
    items = items,
    values_fn = values_fn
  )
}
