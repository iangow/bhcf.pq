#' List BHCF zip files from local storage
#'
#' @param raw_data_dir Optional parent directory containing BHCF zip files.
#'   If NULL, `RAW_DATA_DIR` is used.
#' @param schema Schema subdirectory under `raw_data_dir`. Default is `"bhcf"`.
#'   Set to NULL to use the resolved directory directly.
#'
#' @return A tibble with columns `zipfile` and `date`.
#' @export
bhcf_list_zips <- function(raw_data_dir = NULL, schema = "bhcf") {
  in_dir <- resolve_in_dir(raw_data_dir = raw_data_dir, schema = schema)

  if (is.null(in_dir) || !nzchar(in_dir)) {
    stop("Provide `raw_data_dir` or set `RAW_DATA_DIR`.", call. = FALSE)
  }

  in_dir <- normalizePath(in_dir, mustWork = FALSE)

  files <- list.files(
    in_dir,
    pattern = "^BHCF\\d{8}\\.ZIP$",
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (length(files) == 0L) {
    return(tibble::tibble(
      zipfile = character(0),
      date = as.Date(character(0))
    ))
  }

  date_raw <- stringr::str_extract(basename(files), "\\d{8}")

  tibble::tibble(
    zipfile = normalizePath(files, mustWork = FALSE),
    date = as.Date(date_raw, format = "%Y%m%d")
  ) |>
    dplyr::arrange(.data$date, .data$zipfile)
}

#' @keywords internal
#' @noRd
pick_inner_txt <- function(files) {
  txt <- files[stringr::str_detect(files, "\\.txt$")]
  if (length(txt) == 0L) {
    stop("No .txt file found inside BHCF zip.", call. = FALSE)
  }

  preferred <- txt[stringr::str_detect(basename(txt), "^BHCF\\d{8}\\.txt$")]
  if (length(preferred) > 0L) return(preferred[[1]])

  txt[[1]]
}

#' @keywords internal
#' @noRd
parse_ffiec_date <- function(x) {
  x <- as.character(x)
  x[x %in% c("0", "00000000", "99991231")] <- NA_character_
  as.Date(x, format = "%Y%m%d")
}

#' @keywords internal
#' @noRd
collector_for <- function(x) {
  switch(
    x,
    text = readr::col_character(),
    integer = readr::col_integer(),
    float = readr::col_double(),
    date = readr::col_character(),
    readr::col_guess()
  )
}

#' @keywords internal
#' @noRd
normalize_bhcf_lookup <- function(type_lookup) {
  if (is.null(type_lookup)) {
    if (exists("bhcf_types", inherits = TRUE)) {
      return(
        tibble::as_tibble(get("bhcf_types", inherits = TRUE)) |>
          dplyr::transmute(
            item = as.character(.data$item),
            data_type = tolower(as.character(.data$data_type))
          )
      )
    }
    return(NULL)
  }

  if (inherits(type_lookup, "data.frame")) {
    lookup <- tibble::as_tibble(type_lookup)
  } else if (is.character(type_lookup) && length(type_lookup) == 1L) {
    ext <- tolower(tools::file_ext(type_lookup))
    lookup <- switch(
      ext,
      csv = readr::read_csv(type_lookup, show_col_types = FALSE, progress = FALSE),
      parquet = arrow::read_parquet(type_lookup),
      rds = readRDS(type_lookup),
      stop("Unsupported `type_lookup` extension. Use .csv, .parquet, or .rds.", call. = FALSE)
    )
    lookup <- tibble::as_tibble(lookup)
  } else {
    stop("`type_lookup` must be NULL, a data.frame, or a file path.", call. = FALSE)
  }

  req <- c("item", "data_type")
  miss <- setdiff(req, names(lookup))
  if (length(miss) > 0L) {
    stop("`type_lookup` is missing required columns: ", paste(miss, collapse = ", "), call. = FALSE)
  }

  lookup |>
    dplyr::transmute(
      item = as.character(.data$item),
      data_type = tolower(as.character(.data$data_type))
    )
}

#' @keywords internal
#' @noRd
build_bhcf_colspec <- function(col_names, lookup = NULL) {
  data_types <- tibble::tibble(item = col_names)

  if (!is.null(lookup)) {
    data_types <- dplyr::left_join(data_types, lookup, by = "item")
  } else {
    data_types <- dplyr::mutate(data_types, data_type = NA_character_)
  }

  data_types <- data_types |>
    dplyr::mutate(mnemonic = stringr::str_sub(.data$item, 1, 4)) |>
    dplyr::mutate(
      data_type = dplyr::if_else(
        is.na(.data$data_type) & .data$mnemonic %in% c("BHTX", "TEXT"),
        "text",
        .data$data_type
      ),
      data_type = dplyr::coalesce(.data$data_type, "float")
    )

  collector_list <- stats::setNames(
    purrr::map(data_types$data_type, collector_for),
    data_types$item
  )

  list(
    col_spec = do.call(readr::cols_only, collector_list),
    date_cols = data_types |>
      dplyr::filter(.data$data_type == "date") |>
      dplyr::pull(.data$item)
  )
}

#' @keywords internal
#' @noRd
read_bhcf_zip <- function(zipfile, type_lookup = NULL, reader = NULL) {
  if (!is.null(reader)) {
    df <- reader(zipfile)
    if (!inherits(df, "data.frame")) {
      stop("Custom `reader` must return a data.frame or tibble.", call. = FALSE)
    }
    return(tibble::as_tibble(df))
  }

  inside <- utils::unzip(zipfile, list = TRUE)$Name
  inner_file <- pick_inner_txt(inside)

  header_line <- readr::read_lines(unz(zipfile, inner_file), n_max = 1)

  col_names <- stringr::str_split(header_line, "\\^", simplify = TRUE) |>
    as.character() |>
    stringr::str_remove("^:") |>
    stringr::str_trim()

  lookup <- normalize_bhcf_lookup(type_lookup)
  col_data <- build_bhcf_colspec(col_names, lookup = lookup)

  df <- readr::read_delim(
    unz(zipfile, inner_file),
    delim = "^",
    col_names = col_names,
    skip = 1,
    quote = "",
    na = c("", "NA", "N/A"),
    progress = FALSE,
    show_col_types = FALSE,
    trim_ws = TRUE,
    col_types = col_data$col_spec
  )

  if (length(col_data$date_cols) > 0L) {
    df <- dplyr::mutate(
      df,
      dplyr::across(dplyr::any_of(col_data$date_cols), parse_ffiec_date)
    )
  }

  attr(df, "inner_file") <- inner_file
  tibble::as_tibble(df)
}

#' @keywords internal
#' @noRd
`%||%` <- function(x, y) {
  if (is.null(x) || (length(x) == 1L && is.na(x))) y else x
}

#' @keywords internal
#' @noRd
bhcf_value_type <- function(x) {
  if (inherits(x, "Date")) return("date")
  if (is.logical(x)) return("bool")
  if (is.integer(x)) return("int")
  if (is.double(x)) return("float")
  if (is.character(x)) return("text")
  "other"
}

#' @keywords internal
#' @noRd
make_bhcf_long_files <- function(df, out_dir, date_raw) {
  key_cols <- c("RSSD9001", "date")
  if (!all(key_cols %in% names(df))) {
    stop("Expected key columns `RSSD9001` and `date` in BHCF data.", call. = FALSE)
  }

  value_cols <- setdiff(names(df), key_cols)
  if (length(value_cols) == 0L) {
    return(tibble::tibble(
      type = character(0),
      kind = character(0),
      parquet = character(0),
      n_rows = integer(0)
    ))
  }

  type_map <- vapply(df[value_cols], bhcf_value_type, character(1))
  keep_types <- c("float", "int", "text", "date", "bool")

  purrr::map_dfr(keep_types, function(tp) {
    cols <- value_cols[type_map == tp]
    if (length(cols) == 0L) return(NULL)

    long_df <- df |>
      dplyr::select(dplyr::all_of(c(key_cols, cols))) |>
      tidyr::pivot_longer(
        cols = dplyr::all_of(cols),
        names_to = "item",
        values_to = "value",
        values_drop_na = TRUE
      )

    out_file <- sprintf("bhcf_%s_%s.parquet", tp, date_raw)
    out_path <- file.path(out_dir, out_file)
    arrow::write_parquet(long_df, out_path)

    tibble::tibble(
      type = "long",
      kind = tp,
      parquet = out_file,
      n_rows = nrow(long_df)
    )
  })
}

#' @keywords internal
#' @noRd
process_bhcf_zip <- function(zipfile, out_dir, type_lookup = NULL, reader = NULL,
                             keep_wide = FALSE, keep_long = TRUE) {
  date <- as.Date(
    stringr::str_extract(basename(zipfile), "\\d{8}"),
    format = "%Y%m%d"
  )

  if (is.na(date)) {
    stop(sprintf("Could not parse date from zip file: %s", basename(zipfile)), call. = FALSE)
  }

  df <- read_bhcf_zip(zipfile, type_lookup = type_lookup, reader = reader)

  if (!"date" %in% names(df)) {
    df <- dplyr::mutate(df, date = date)
  }

  date_raw <- format(date, "%Y%m%d")

  long_meta <- if (isTRUE(keep_long)) {
    make_bhcf_long_files(df = df, out_dir = out_dir, date_raw = date_raw)
  } else {
    tibble::tibble(
      type = character(0),
      kind = character(0),
      parquet = character(0),
      n_rows = integer(0)
    )
  }

  wide_meta <- if (isTRUE(keep_wide)) {
    out_file <- sprintf("bhcf_%s.parquet", date_raw)
    out_path <- file.path(out_dir, out_file)
    arrow::write_parquet(df, out_path)

    tibble::tibble(
      type = "wide",
      kind = "bhcf",
      parquet = out_file,
      n_rows = nrow(df)
    )
  } else {
    tibble::tibble(
      type = character(0),
      kind = character(0),
      parquet = character(0),
      n_rows = integer(0)
    )
  }

  dplyr::bind_rows(long_meta, wide_meta) |>
    dplyr::mutate(
      date = date,
      zipfile = basename(zipfile),
      inner_files = list(attr(df, "inner_file") %||% NA_character_),
      n_cols = ncol(df)
    ) |>
    dplyr::relocate(.data$type, .data$kind, .data$date, .data$parquet)
}

#' Process BHCF bulk data into Parquet files
#'
#' @param zipfiles Optional character vector of BHCF zip file paths. If NULL,
#'   zip files are discovered from the resolved raw-data directory.
#' @param raw_data_dir Optional parent directory containing raw BHCF zip files.
#'   If NULL, `RAW_DATA_DIR` is used.
#' @param data_dir Optional parent directory for output Parquet files. If NULL,
#'   `DATA_DIR` is used.
#' @param schema Schema subdirectory for input and output resolution.
#'   Default is `"bhcf"`. Set to NULL to avoid schema subdirectories.
#' @param keep_process_data Logical; whether to write `bhcf_process_data.parquet`
#'   in the output directory.
#' @param keep_wide Logical; whether to write wide per-quarter parquet files
#'   (`bhcf_YYYYMMDD.parquet`). Default is `FALSE`.
#' @param keep_long Logical; whether to write long per-quarter parquet files by
#'   data type (`bhcf_float_YYYYMMDD.parquet`, etc.). Default is `TRUE`.
#' @param type_lookup Optional lookup used to assign BHCF column types. Supply
#'   either a data frame (with columns `item` and `data_type`) or a path to
#'   `.csv`, `.parquet`, or `.rds`.
#' @param reader Optional custom function with signature `function(zipfile)` that
#'   returns a data frame for one zip file.
#'
#' @return A tibble describing written Parquet files.
#' @export
bhcf_process <- function(zipfiles = NULL,
                         raw_data_dir = NULL,
                         data_dir = NULL,
                         schema = "bhcf",
                         keep_process_data = NULL,
                         keep_wide = FALSE,
                         keep_long = TRUE,
                         type_lookup = NULL,
                         reader = NULL) {

  zipfiles_was_null <- is.null(zipfiles)

  dirs <- resolve_dirs(raw_data_dir = raw_data_dir, data_dir = data_dir, schema = schema)
  in_dir <- dirs$in_dir
  out_dir <- dirs$out_dir

  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set `DATA_DIR`.", call. = FALSE)
  }

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (is.null(zipfiles)) {
    if (is.null(in_dir)) {
      stop("Provide `zipfiles`, or `raw_data_dir`, or set `RAW_DATA_DIR`.", call. = FALSE)
    }

    zipfiles <- bhcf_list_zips(raw_data_dir = raw_data_dir, schema = schema)$zipfile

    if (length(zipfiles) == 0L) {
      stop("No BHCF zip files found in the resolved input directory.", call. = FALSE)
    }
  }

  zipfiles <- normalizePath(zipfiles, mustWork = TRUE)

  if (is.null(keep_process_data)) {
    keep_process_data <- zipfiles_was_null
  }

  out <- purrr::map_dfr(
    zipfiles,
    process_bhcf_zip,
    out_dir = out_dir,
    keep_wide = keep_wide,
    keep_long = keep_long,
    type_lookup = type_lookup,
    reader = reader
  )

  if (isTRUE(keep_process_data)) {
    arrow::write_parquet(out, file.path(out_dir, "bhcf_process_data.parquet"))
  }

  attr(out, "out_dir") <- out_dir
  out
}

#' List generated BHCF Parquet files
#'
#' @param data_dir Optional parent directory for Parquet output.
#' @param schema Schema subdirectory. Default is `"bhcf"`.
#' @param all_files Logical; if TRUE, return all Parquet files.
#'
#' @return A tibble with `base_name`, `full_name`, and `kind`.
#' @export
bhcf_list_pqs <- function(data_dir = NULL,
                          schema = "bhcf",
                          all_files = FALSE) {

  out_dir <- resolve_out_dir(data_dir = data_dir, schema = schema)
  if (is.null(out_dir)) {
    stop("Provide `data_dir` or set `DATA_DIR`.", call. = FALSE)
  }

  out_dir <- normalizePath(out_dir, mustWork = FALSE)

  res <- list.files(out_dir, pattern = "\\.parquet$", full.names = TRUE)

  if (!isTRUE(all_files)) {
    keep <- basename(res) |>
      stringr::str_detect("^bhcf_(float|int|text|date|bool)_\\d{8}\\.parquet$|^bhcf_\\d{8}\\.parquet$")
    res <- res[keep]
  }

  tibble::tibble(
    base_name = basename(res),
    full_name = res,
    kind = dplyr::case_when(
      stringr::str_detect(basename(res), "^bhcf_(float|int|text|date|bool)_\\d{8}\\.parquet$") ~
        stringr::str_match(basename(res), "^bhcf_(float|int|text|date|bool)_\\d{8}\\.parquet$")[, 2],
      stringr::str_detect(basename(res), "^bhcf_\\d{8}\\.parquet$") ~ "wide",
      TRUE ~ NA_character_
    )
  )
}
