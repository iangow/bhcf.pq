#' Pivot long BHCF data to wide format using DuckDB
#'
#' Pivots a DuckDB-backed long BHCF table (typically with columns like
#' `RSSD9001`, `date`, `item`, `value`) into wide format.
#'
#' @param data A DuckDB-backed lazy `tbl` containing long BHCF data.
#' @param id_cols Character vector of identifier columns in the wide output.
#'   Defaults to `c("RSSD9001", "date")`.
#' @param names_from Character scalar with the column that contains variable
#'   names (default `"item"`).
#' @param values_from Character scalar with the column that contains values
#'   (default `"value"`).
#' @param items Optional character vector of item names to include.
#' @param values_fn Character scalar naming the DuckDB aggregate used when
#'   multiple rows exist per `id_cols + names_from` combination. Default is
#'   `"first"`.
#'
#' @return A DuckDB-backed lazy `tbl` in wide format.
#' @export
bhcf_pivot <- function(data,
                       id_cols = c("RSSD9001", "date"),
                       names_from = "item",
                       values_from = "value",
                       items = NULL,
                       values_fn = "first") {

  stopifnot(
    inherits(data, "tbl"),
    is.character(id_cols), length(id_cols) >= 1L,
    is.character(names_from), length(names_from) == 1L,
    is.character(values_from), length(values_from) == 1L
  )

  conn <- dbplyr::remote_con(data)
  stopifnot(DBI::dbIsValid(conn))

  x_sql <- dbplyr::sql_render(data)

  in_list_sql <- NULL
  where_sql <- ""
  if (!is.null(items)) {
    stopifnot(is.character(items), length(items) >= 1L)
    in_list_sql <- paste(DBI::dbQuoteString(conn, items), collapse = ", ")
    where_sql <- paste0("WHERE ", names_from, " IN (", in_list_sql, ")\n")
  }

  base_sql <- paste0(
    "SELECT ", paste(c(id_cols, names_from, values_from), collapse = ", "), "\n",
    "FROM (", x_sql, ") AS x\n",
    where_sql
  )

  on_sql <- if (!is.null(in_list_sql)) {
    paste0(names_from, " IN (", in_list_sql, ")")
  } else {
    names_from
  }

  sql <- paste0(
    "SELECT *\n",
    "FROM (\n",
    "  PIVOT (", base_sql, ")\n",
    "  ON ", on_sql, "\n",
    "  USING ", values_fn, "(", values_from, ")\n",
    "  GROUP BY ", paste(id_cols, collapse = ", "), "\n",
    ") AS p"
  )

  dplyr::tbl(conn, dplyr::sql(sql))
}
