# Tests for export_passaging_subtree_bundle(), export_passaging_subtree_sql()
# (thin compatibility wrapper), and their shared internal helpers.
#
# No live MySQL connection is required.  All DB interactions are intercepted
# via testthat::with_mocked_bindings() targeting the cloneid namespace.
#
# DBI::ANSI() is used as a lightweight connection-like object wherever a conn
# argument is needed only for DBI::dbQuoteString() — it performs standard SQL
# quoting (single-quoted strings, doubled-single-quote escaping) without
# opening any real database connection.

library(testthat)
library(DBI)  # for DBI::ANSI()

# Shorthand for accessing internal helpers without typing cloneid::: every time.
.sql_val      <- cloneid:::.sql_val
.r2i          <- cloneid:::.rows_to_inserts
.subtree      <- cloneid:::.subtree_ids
.policy       <- cloneid:::.apply_external_parent_policy
.dep_closure  <- cloneid:::.fetch_dependency_closure
.persp_closure<- cloneid:::.fetch_perspective_closure
.storage_rows   <- cloneid:::.fetch_storage_rows
.decode_profiles <- cloneid:::.decode_perspective_profiles
.db_fetch_sym <- "cloneid:::.db_fetch"

# Reusable lightweight mock connection (no real DB).
mc <- DBI::ANSI()

# ---------------------------------------------------------------------------
# Helper that builds a minimal Passaging data frame for a given id.
# All FK columns are NA so no dependency rows are needed in mocks.
# ---------------------------------------------------------------------------
.passaging_row <- function(id, from1 = NA_character_, from2 = NA_character_) {
  data.frame(
    id                  = as.character(id),
    passaged_from_id1   = as.character(from1),
    passaged_from_id2   = as.character(from2),
    cellLine            = NA_character_,
    flask               = NA_integer_,
    media               = NA_integer_,
    stringsAsFactors    = FALSE
  )
}

# ---------------------------------------------------------------------------
# .sql_val
# ---------------------------------------------------------------------------

test_that(".sql_val returns NULL for NULL input", {
  expect_equal(.sql_val(NULL, mc), "NULL")
})

test_that(".sql_val returns NULL for NA input", {
  expect_equal(.sql_val(NA,    mc), "NULL")
  expect_equal(.sql_val(NA_character_, mc), "NULL")
  expect_equal(.sql_val(NA_real_,      mc), "NULL")
})

test_that(".sql_val returns NULL for zero-length vector", {
  expect_equal(.sql_val(character(0), mc), "NULL")
})

test_that(".sql_val stops on length > 1", {
  expect_error(.sql_val(c(1, 2), mc), "scalar")
})

test_that(".sql_val converts numeric without scientific notation", {
  expect_equal(.sql_val(1234567890.5, mc), "1234567890.5")
  expect_equal(.sql_val(0.000001,     mc), "0.000001")
  expect_equal(.sql_val(1e12,         mc), "1000000000000")
})

test_that(".sql_val converts integer", {
  expect_equal(.sql_val(42L, mc), "42")
})

test_that(".sql_val converts logical to 0/1", {
  expect_equal(.sql_val(TRUE,  mc), "1")
  expect_equal(.sql_val(FALSE, mc), "0")
})

test_that(".sql_val quotes character with single quotes and escapes embedded quotes", {
  expect_equal(.sql_val("hello", mc), "'hello'")
  expect_equal(.sql_val("it's", mc),  "'it''s'")
})

test_that(".sql_val formats Date", {
  d <- as.Date("2024-06-15")
  expect_equal(.sql_val(d, mc), "'2024-06-15'")
})

test_that(".sql_val formats POSIXct as YYYY-MM-DD HH:MM:SS", {
  ts <- as.POSIXct("2024-06-15 13:45:00", tz = "UTC")
  result <- .sql_val(ts, mc)
  expect_match(result, "^'2024-06-15 13:45:00'$")
})

# --- blob round-trip via raw vector (RMySQL returns mediumblob as raw) -------

test_that(".sql_val: raw vector → hex literal (blob round-trip)", {
  # Known bytes: 0x41 0x42 0x43 = "ABC"
  raw_input <- as.raw(c(0x41, 0x42, 0x43))
  result <- .sql_val(raw_input, mc)
  expect_equal(result, "0x414243")
})

test_that(".sql_val: list-of-one-raw → hex literal (RMySQL mediumblob format)", {
  # RMySQL returns mediumblob columns as list columns; each cell is a raw vector.
  raw_bytes <- as.raw(c(0xDE, 0xAD, 0xBE, 0xEF))
  result <- .sql_val(list(raw_bytes), mc)
  expect_equal(result, "0xDEADBEEF")
})

test_that(".sql_val: empty raw vector → NULL", {
  expect_equal(.sql_val(raw(0), mc), "NULL")
})

test_that(".sql_val: list-of-NULL → NULL", {
  expect_equal(.sql_val(list(NULL), mc), "NULL")
})

test_that(".sql_val: blob=TRUE on character → hex literal, not quoted string", {
  # 'AB' = 0x4142 in ASCII
  result <- .sql_val("AB", mc, blob = TRUE)
  expect_equal(result, "0x4142")
})

test_that(".sql_val: blob=TRUE on empty string → NULL", {
  expect_equal(.sql_val("", mc, blob = TRUE), "NULL")
})

# ---------------------------------------------------------------------------
# .rows_to_inserts
# ---------------------------------------------------------------------------

test_that(".rows_to_inserts: empty data frame returns character(0)", {
  df <- data.frame(id = integer(0), name = character(0), stringsAsFactors = FALSE)
  expect_identical(.r2i("MyTable", df, mc), character(0))
})

test_that(".rows_to_inserts: single row produces a well-formed INSERT", {
  df <- data.frame(id = 1L, name = "DMEM", val = 3.14, stringsAsFactors = FALSE)
  stmts <- .r2i("Media", df, mc)
  expect_length(stmts, 1L)
  expect_match(stmts, "^INSERT INTO `Media`")
  expect_match(stmts, "`id`, `name`, `val`")
  expect_match(stmts, "1, 'DMEM', 3.14")
  expect_match(stmts, "VALUES \\(")
  expect_match(stmts, "\\);$")
})

test_that(".rows_to_inserts: multiple rows produce one INSERT per row", {
  df <- data.frame(id = 1:3, name = c("A", "B", "C"), stringsAsFactors = FALSE)
  stmts <- .r2i("Flask", df, mc)
  expect_length(stmts, 3L)
})

test_that(".rows_to_inserts: NA values become NULL in INSERT", {
  df <- data.frame(id = 1L, parent = NA_integer_, stringsAsFactors = FALSE)
  stmts <- .r2i("Passaging", df, mc)
  expect_match(stmts, "1, NULL")
})

# --- targeted blob column test (Loci.content) --------------------------------

test_that(".rows_to_inserts: blob column (list-of-raw) emitted as 0x hex literal", {
  # Simulate the RMySQL list-column format for Loci.content (mediumblob).
  # The bytes represent a small pretend profile payload.
  blob_bytes  <- as.raw(c(0x01, 0x02, 0x03, 0xFF))
  content_col <- list(blob_bytes)  # single row, list-column cell

  df <- data.frame(
    id      = 99L,
    content = I(content_col),       # I() preserves list class
    stringsAsFactors = FALSE
  )

  stmts <- .r2i("Loci", df, mc, blob_cols = "content")
  expect_length(stmts, 1L)
  # Blob should appear as 0x01 02 03 FF hex literal, not as a quoted string.
  expect_match(stmts, "0x010203FF", fixed = TRUE)
  # Make sure the value is NOT wrapped in single quotes.
  expect_false(grepl("'0x", stmts, fixed = TRUE))
})

test_that(".rows_to_inserts: non-blob character column with blob_cols specified stays quoted", {
  df <- data.frame(id = 1L, name = "hello", content = I(list(as.raw(0x41))),
                   stringsAsFactors = FALSE)
  stmts <- .r2i("Loci", df, mc, blob_cols = "content")
  # name should still be quoted
  expect_match(stmts, "'hello'")
  # content should be hex
  expect_match(stmts, "0x41", fixed = TRUE)
})

# ---------------------------------------------------------------------------
# .subtree_ids
# ---------------------------------------------------------------------------

# Helper to build a mock .db_fetch dispatcher for subtree tests.
# `responses` is a named list: pattern (regex) -> data.frame to return.
.make_fetch_mock <- function(responses) {
  function(stmt, conn = NULL) {
    for (pat in names(responses)) {
      if (grepl(pat, stmt, fixed = FALSE)) return(responses[[pat]])
    }
    stop("Unexpected query in mock_fetch: ", stmt)
  }
}

test_that(".subtree_ids: missing root triggers stop()", {
  mock <- .make_fetch_mock(list(
    "SELECT id FROM Passaging WHERE id" = data.frame(id = character(0),
                                                      stringsAsFactors = FALSE)
  ))
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code      = expect_error(.subtree(root_id = "999", conn = mc), "not found")
  )
})

test_that(".subtree_ids: leaf node (no children) returns only root", {
  root_resp  <- data.frame(id = "1", stringsAsFactors = FALSE)
  child_resp <- data.frame(id = character(0), stringsAsFactors = FALSE)
  call_count <- 0L

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =", stmt))              return(root_resp)
    if (grepl("passaged_from_id1 =", stmt))     return(child_resp)
    stop("Unexpected: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .subtree(root_id = "1", recursive = FALSE, conn = mc)
      expect_equal(result, "1")
    }
  )
})

test_that(".subtree_ids: recursive=FALSE returns root + direct children only", {
  root_resp    <- data.frame(id = "1", stringsAsFactors = FALSE)
  children_1   <- data.frame(id = c("2", "3"), stringsAsFactors = FALSE)
  no_children  <- data.frame(id = character(0), stringsAsFactors = FALSE)

  # recursive=FALSE: child query is called once for root, then NOT recursed.
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =", stmt))          return(root_resp)
    if (grepl("passaged_from_id1 =", stmt)) return(children_1)
    stop("Unexpected: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .subtree(root_id = "1", recursive = FALSE, conn = mc)
      expect_setequal(result, c("1", "2", "3"))
    }
  )
})

test_that(".subtree_ids: recursive=TRUE returns full closure", {
  root_resp     <- data.frame(id = "1", stringsAsFactors = FALSE)
  children_of_1 <- data.frame(id = "2",             stringsAsFactors = FALSE)
  children_of_2 <- data.frame(id = "3",             stringsAsFactors = FALSE)
  no_children   <- data.frame(id = character(0),    stringsAsFactors = FALSE)

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =", stmt))              return(root_resp)
    if (grepl("passaged_from_id1 = '1'", stmt)) return(children_of_1)
    if (grepl("passaged_from_id1 = '2'", stmt)) return(children_of_2)
    if (grepl("passaged_from_id1 = '3'", stmt)) return(no_children)
    stop("Unexpected: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .subtree(root_id = "1", recursive = TRUE, conn = mc)
      expect_setequal(result, c("1", "2", "3"))
      # root must be first
      expect_equal(result[1], "1")
    }
  )
})

# ---------------------------------------------------------------------------
# .apply_external_parent_policy
# ---------------------------------------------------------------------------

test_that("policy='error': self-contained set (no external refs) passes unchanged", {
  rows <- .passaging_row("2", from1 = "1")
  result <- .policy(rows, export_ids = c("1", "2"),
                    policy = "error", conn = NULL)
  expect_equal(result$export_ids,           c("1", "2"))
  expect_equal(nrow(result$detached_parent_refs), 0L)
  expect_equal(result$passaging_rows$passaged_from_id1, "1")
})

test_that("policy='error': external ref triggers stop() with the offending id", {
  rows <- .passaging_row("2", from1 = "99")  # 99 not in export_ids
  expect_error(
    .policy(rows, export_ids = "2", policy = "error", conn = NULL),
    "99"
  )
})

test_that("policy='nullify': external ref is set to NA and logged", {
  rows <- .passaging_row("2", from1 = "99")
  result <- .policy(rows, export_ids = "2", policy = "nullify", conn = NULL)

  # passaged_from_id1 must be NA after nullification
  expect_true(is.na(result$passaging_rows$passaged_from_id1))

  # detached_parent_refs must capture one row with correct metadata
  refs <- result$detached_parent_refs
  expect_equal(nrow(refs), 1L)
  expect_equal(refs$passaging_id,   "2")
  expect_equal(refs$field,          "passaged_from_id1")
  expect_equal(refs$original_value, "99")
})

test_that("policy='nullify': internal refs are left untouched", {
  rows <- rbind(
    .passaging_row("1"),
    .passaging_row("2", from1 = "1")
  )
  result <- .policy(rows, export_ids = c("1", "2"),
                    policy = "nullify", conn = NULL)
  expect_equal(result$passaging_rows$passaged_from_id1[2], "1")
  expect_equal(nrow(result$detached_parent_refs), 0L)
})

test_that("policy='include': pulls in missing parents until closure", {
  child_row  <- .passaging_row("2", from1 = "1")
  parent_row <- .passaging_row("1")  # no parents

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE `id` IN", stmt)) return(parent_row)
    stop("Unexpected: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .policy(child_row, export_ids = "2",
                        policy = "include", conn = mc)
      expect_setequal(result$export_ids, c("2", "1"))
      expect_equal(nrow(result$passaging_rows), 2L)
      expect_equal(nrow(result$detached_parent_refs), 0L)
    }
  )
})

test_that("policy='include': referenced parent not in DB triggers stop()", {
  child_row <- .passaging_row("2", from1 = "999")
  no_rows   <- .passaging_row("2")[0, ]  # zero-row df with right schema

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE `id` IN", stmt)) return(no_rows)
    stop("Unexpected: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      expect_error(
        .policy(child_row, export_ids = "2", policy = "include", conn = mc),
        "999"
      )
    }
  )
})

# ---------------------------------------------------------------------------
# export_passaging_subtree_bundle / export_passaging_subtree_sql — integration
#
# All DB interactions are mocked.  We exercise the orchestration logic,
# return structure, file-output contract, and the include_perspectives /
# include_storage flags for both format="sql" (via wrapper and directly) and
# format="rds".
# ---------------------------------------------------------------------------

# Build a minimal but complete mock for all DB calls made by the function.
# Root id = "1", leaf node (no children), no FK dependencies, no storage/perspectives.
.make_full_mock <- function() {
  empty_id_df  <- data.frame(id = character(0), stringsAsFactors = FALSE)
  root_id_df   <- data.frame(id = "1",          stringsAsFactors = FALSE)
  passaging_df <- .passaging_row("1")

  function(stmt, conn = NULL) {
    # ── .subtree_ids queries ────────────────────────────────────────────────
    if (grepl("WHERE id =",              stmt, fixed = FALSE)) return(root_id_df)
    if (grepl("passaged_from_id1 =",    stmt, fixed = FALSE)) return(empty_id_df)
    # ── SELECT * FROM Passaging WHERE id IN ─────────────────────────────────
    if (grepl("SELECT \\* FROM `Passaging`", stmt))            return(passaging_df)
    # ── dependency closure ──────────────────────────────────────────────────
    if (grepl("WHERE 1=0|WHERE `name` IN|WHERE `id` IN \\(1\\)|MediaIngredients", stmt))
      return(data.frame())
    # ── perspective closure ─────────────────────────────────────────────────
    if (grepl("Perspective WHERE `origin` IN", stmt))
      return(data.frame(cloneID = integer(0), origin = character(0),
                        profile_loci = integer(0), stringsAsFactors = FALSE))
    if (grepl("FROM `Perspective` WHERE `origin` IN", stmt) &&
        grepl("GROUP BY `origin`, `whichPerspective`", stmt))
      return(data.frame(passaging_id = character(0), perspective_type = character(0),
                        row_count = integer(0), stringsAsFactors = FALSE))
    # ── storage ─────────────────────────────────────────────────────────────
    if (grepl("LiquidNitrogen|Minus80Freezer", stmt))
      return(data.frame())
    stop("Unexpected query in full mock: ", stmt)
  }
}

test_that("export_passaging_subtree_sql returns list with expected names", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id   = "1",
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_named(result, c("metadata", "export_ids", "detached_parent_refs",
                              "tables", "manifest_template", "asset_inventory",
                              "decoded", "sql", "statements"))
    }
  )
})

test_that("statements vector starts with START TRANSACTION and ends with COMMIT", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id   = "1",
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      stmts <- result$statements
      expect_equal(stmts[1],            "START TRANSACTION;")
      expect_equal(stmts[length(stmts)], "COMMIT;")
    }
  )
})

test_that("sql string contains header comment block", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id   = "1",
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_match(result$sql, "-- CLONEID subtree export")
      expect_match(result$sql, "root_id:")
      expect_match(result$sql, "exported_ids:")
    }
  )
})

test_that("file= writes the formatted sql string and overwrites unconditionally", {
  mock <- .make_full_mock()
  tmp  <- tempfile(fileext = ".sql")
  on.exit(unlink(tmp), add = TRUE)

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id   = "1",
        file = tmp,
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      # File must exist and content must match result$sql exactly
      expect_true(file.exists(tmp))
      written <- paste(readLines(tmp, warn = FALSE), collapse = "\n")
      expect_equal(written, result$sql)

      # Overwrite: calling again must not error and must update the file
      result2 <- export_passaging_subtree_sql(
        id   = "1",
        file = tmp,
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      written2 <- paste(readLines(tmp, warn = FALSE), collapse = "\n")
      expect_equal(written2, result2$sql)
    }
  )
})

test_that("include_perspectives=FALSE: no Perspective or Loci inserts in statements", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id                   = "1",
        conn                 = mc,
        include_perspectives = FALSE,
        include_storage      = FALSE
      )
      stmts_str <- paste(result$statements, collapse = "\n")
      expect_false(grepl("INSERT INTO `Perspective`", stmts_str))
      expect_false(grepl("INSERT INTO `Loci`",        stmts_str))
      expect_equal(nrow(result$tables$Perspective), 0L)
      expect_equal(nrow(result$tables$Loci),        0L)
    }
  )
})

test_that("include_storage=FALSE: no storage-table inserts in statements", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id                   = "1",
        conn                 = mc,
        include_perspectives = FALSE,
        include_storage      = FALSE
      )
      stmts_str <- paste(result$statements, collapse = "\n")
      expect_false(grepl("LiquidNitrogen", stmts_str))
      expect_false(grepl("Minus80Freezer", stmts_str))
      expect_equal(nrow(result$tables$LiquidNitrogen), 0L)
      expect_equal(nrow(result$tables$Minus80Freezer), 0L)
    }
  )
})

test_that("export_ids in return matches the mocked subtree", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id                   = "1",
        conn                 = mc,
        include_perspectives = FALSE,
        include_storage      = FALSE
      )
      expect_equal(result$export_ids, "1")
    }
  )
})

test_that("detached_parent_refs is empty when root has no parent refs", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_sql(
        id                   = "1",
        conn                 = mc,
        include_perspectives = FALSE,
        include_storage      = FALSE
      )
      expect_equal(nrow(result$detached_parent_refs), 0L)
    }
  )
})

test_that("missing root id triggers stop()", {
  mock <- .make_fetch_mock(list(
    "WHERE id =" = data.frame(id = character(0), stringsAsFactors = FALSE)
  ))
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      expect_error(
        export_passaging_subtree_sql(
          id   = "999",
          conn = mc,
          include_perspectives = FALSE,
          include_storage      = FALSE
        ),
        "not found"
      )
    }
  )
})

# ---------------------------------------------------------------------------
# .fetch_dependency_closure
# ---------------------------------------------------------------------------

# Minimal helpers for building dependency-closure test data frames.
.cl_row <- function(name) data.frame(name = name, stringsAsFactors = FALSE)
.flask_row <- function(id) data.frame(id = as.integer(id), stringsAsFactors = FALSE)
.media_row <- function(id, base1 = NA_character_) {
  data.frame(id = as.integer(id), base1 = base1,
             base2 = NA_character_, FBS = NA_character_,
             EnergySource2 = NA_character_, EnergySource = NA_character_,
             HEPES = NA_character_, Salt = NA_character_,
             antibiotic = NA_character_, antibiotic2 = NA_character_,
             antimycotic = NA_character_, Stressor = NA_character_,
             antibiotic3 = NA_character_, antibiotic4 = NA_character_,
             stringsAsFactors = FALSE)
}
.mi_row <- function(name) data.frame(name = name, stringsAsFactors = FALSE)
.empty_df <- data.frame()

test_that(".fetch_dependency_closure: fetches CellLinesAndPatients for referenced cellLine", {
  rows <- .passaging_row("1")
  rows$cellLine <- "MCF7"
  captured_stmt <- NULL
  mock <- function(stmt, conn = NULL) {
    captured_stmt <<- stmt
    if (grepl("CellLinesAndPatients",    stmt)) return(.cl_row("MCF7"))
    if (grepl("Flask|Media|Ingredient",  stmt)) return(.empty_df)
    if (grepl("WHERE 1=0",               stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .dep_closure(rows, conn = mc)
      expect_equal(result$cell_lines$name, "MCF7")
    }
  )
})

test_that(".fetch_dependency_closure: missing CellLinesAndPatients row triggers stop() with name", {
  rows <- .passaging_row("1")
  rows$cellLine <- "MISSING_LINE"
  mock <- function(stmt, conn = NULL) {
    # Return empty — simulates the row not existing in the DB.
    if (grepl("CellLinesAndPatients", stmt)) return(data.frame(name = character(0), stringsAsFactors = FALSE))
    if (grepl("WHERE 1=0",            stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = expect_error(.dep_closure(rows, conn = mc), "missing_line")
  )
})

test_that(".fetch_dependency_closure: fetches Flask for referenced flask id", {
  rows <- .passaging_row("1")
  rows$flask <- 7L
  mock <- function(stmt, conn = NULL) {
    if (grepl("CellLinesAndPatients", stmt)) return(.empty_df)
    if (grepl("Flask",                stmt)) return(.flask_row(7L))
    if (grepl("Media|WHERE 1=0",      stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .dep_closure(rows, conn = mc)
      expect_equal(as.integer(result$flasks$id), 7L)
    }
  )
})

test_that(".fetch_dependency_closure: missing Flask row triggers stop() with id", {
  rows <- .passaging_row("1")
  rows$flask <- 99L
  mock <- function(stmt, conn = NULL) {
    if (grepl("CellLinesAndPatients", stmt)) return(.empty_df)
    if (grepl("Flask",                stmt)) return(data.frame(id = integer(0), stringsAsFactors = FALSE))
    if (grepl("WHERE 1=0",            stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = expect_error(.dep_closure(rows, conn = mc), "99")
  )
})

test_that(".fetch_dependency_closure: fetches Media and its MediaIngredients", {
  rows <- .passaging_row("1")
  rows$media <- 3L
  mock <- function(stmt, conn = NULL) {
    if (grepl("CellLinesAndPatients", stmt)) return(.empty_df)
    if (grepl("Flask|WHERE 1=0",      stmt)) return(.empty_df)
    # MediaIngredients must be checked before Media: "Media" is a substring of
    # "MediaIngredients" when using a non-backtick-exact pattern.
    if (grepl("MediaIngredients",     stmt)) return(.mi_row("DMEM"))
    if (grepl("Media",                stmt)) return(.media_row(3L, base1 = "DMEM"))
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .dep_closure(rows, conn = mc)
      expect_equal(as.integer(result$media$id), 3L)
      expect_equal(result$media_ingredients$name, "DMEM")
    }
  )
})

test_that(".fetch_dependency_closure: missing Media row triggers stop() with id", {
  rows <- .passaging_row("1")
  rows$media <- 42L
  mock <- function(stmt, conn = NULL) {
    if (grepl("CellLinesAndPatients", stmt)) return(.empty_df)
    if (grepl("Flask|WHERE 1=0",      stmt)) return(.empty_df)
    if (grepl("MediaIngredients",     stmt)) return(.empty_df)
    if (grepl("Media",                stmt)) return(data.frame(id = integer(0), stringsAsFactors = FALSE))
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = expect_error(.dep_closure(rows, conn = mc), "42")
  )
})

test_that(".fetch_dependency_closure: missing MediaIngredients row triggers stop() with name", {
  rows <- .passaging_row("1")
  rows$media <- 3L
  mock <- function(stmt, conn = NULL) {
    if (grepl("CellLinesAndPatients", stmt)) return(.empty_df)
    if (grepl("Flask|WHERE 1=0",      stmt)) return(.empty_df)
    if (grepl("MediaIngredients",     stmt)) return(data.frame(name = character(0), stringsAsFactors = FALSE))
    if (grepl("Media",                stmt)) return(.media_row(3L, base1 = "GHOST_BASE"))
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = expect_error(.dep_closure(rows, conn = mc), "ghost_base")
  )
})

test_that(".fetch_dependency_closure: all-NA FK columns produce zero-row result tables", {
  rows <- .passaging_row("1")  # cellLine, flask, media all NA
  mock <- function(stmt, conn = NULL) {
    # Only WHERE 1=0 schema queries should be issued.
    if (grepl("WHERE 1=0", stmt)) return(.empty_df)
    stop("Unexpected non-schema query with all-NA FKs: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .dep_closure(rows, conn = mc)
      expect_equal(nrow(result$cell_lines),        0L)
      expect_equal(nrow(result$flasks),            0L)
      expect_equal(nrow(result$media),             0L)
      expect_equal(nrow(result$media_ingredients), 0L)
    }
  )
})

# ---------------------------------------------------------------------------
# .fetch_perspective_closure
# ---------------------------------------------------------------------------

# Build a minimal Perspective data frame row.
.persp_row <- function(clone_id, origin, profile_loci = NA_integer_) {
  data.frame(
    cloneID      = as.integer(clone_id),
    origin       = as.character(origin),
    profile_loci = as.integer(profile_loci),
    profile      = I(list(as.raw(0x00))),  # minimal blob placeholder
    stringsAsFactors = FALSE
  )
}
.loci_row <- function(id) {
  data.frame(
    id      = as.integer(id),
    content = I(list(as.raw(c(0xAB, 0xCD)))),
    stringsAsFactors = FALSE
  )
}

test_that(".fetch_perspective_closure: Perspective rows whose origin is in export_ids are included", {
  export_ids <- c("1", "2")
  persp_data <- rbind(
    .persp_row(10L, "1"),
    .persp_row(11L, "2")
  )
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) return(persp_data)
    # No Loci refs (profile_loci is NA in both rows)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .persp_closure(export_ids, conn = mc)
      expect_equal(nrow(result$perspectives), 2L)
      expect_setequal(result$perspectives$cloneID, c(10L, 11L))
    }
  )
})

test_that(".fetch_perspective_closure: SQL IN clause contains only the supplied export_ids", {
  # Verify the query sent to .db_fetch contains exactly the expected ids and
  # no others — this is the mechanism that excludes unrelated Perspective rows.
  export_ids    <- c("5", "7")
  captured_stmt <- NULL
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) { captured_stmt <<- stmt; return(.persp_row(20L, "5")) }
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      .persp_closure(export_ids, conn = mc)
      # Both ids must appear in the IN clause.
      expect_match(captured_stmt, "'5'")
      expect_match(captured_stmt, "'7'")
      # Unrelated ids must NOT appear.
      expect_false(grepl("'1'|'2'|'3'|'4'|'6'", captured_stmt))
    }
  )
})

test_that(".fetch_perspective_closure: referenced Loci rows are included exactly once", {
  # Two Perspective rows both reference the same Loci id (10).
  # The Loci table should be queried once and return exactly one row.
  persp_data <- rbind(
    .persp_row(20L, "1", profile_loci = 10L),
    .persp_row(21L, "2", profile_loci = 10L)
  )
  loci_data  <- .loci_row(10L)
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) return(persp_data)
    if (grepl("Loci",        stmt)) return(loci_data)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .persp_closure(c("1", "2"), conn = mc)
      # Exactly one Loci row, not two.
      expect_equal(nrow(result$loci), 1L)
      expect_equal(as.integer(result$loci$id), 10L)
    }
  )
})

test_that(".fetch_perspective_closure: Perspective rows with NULL profile_loci produce no Loci query", {
  # profile_loci = NA → no Loci ids to look up → no Loci query should be sent.
  persp_data <- .persp_row(30L, "3", profile_loci = NA_integer_)
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) return(persp_data)
    # If a real Loci IN-query were issued despite no profile_loci, the test fails here.
    if (grepl("Loci",        stmt)) stop("Unexpected Loci query with no profile_loci refs")
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .persp_closure("3", conn = mc)
      expect_equal(nrow(result$loci), 0L)
    }
  )
})

test_that(".fetch_perspective_closure: unrelated Loci ids are excluded (query uses exact IN set)", {
  # Perspective references Loci id 55.  The mock only returns id 55.
  # We verify the IN clause contains '55' and NOT an unrelated id.
  persp_data    <- .persp_row(40L, "4", profile_loci = 55L)
  loci_data     <- .loci_row(55L)
  captured_stmt <- NULL
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) return(persp_data)
    if (grepl("Loci",        stmt)) { captured_stmt <<- stmt; return(loci_data) }
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .persp_closure("4", conn = mc)
      expect_equal(as.integer(result$loci$id), 55L)
      # Loci query should target id 55, not any other id.
      expect_match(captured_stmt, "55")
      expect_false(grepl("56|57|100", captured_stmt))
    }
  )
})

test_that(".fetch_perspective_closure: missing Loci row triggers stop() with id", {
  persp_data <- .persp_row(50L, "5", profile_loci = 99L)
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0",   stmt)) return(.empty_df)
    if (grepl("Perspective", stmt)) return(persp_data)
    if (grepl("Loci",        stmt)) return(data.frame(id = integer(0), stringsAsFactors = FALSE))
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = expect_error(.persp_closure("5", conn = mc), "99")
  )
})

test_that(".fetch_perspective_closure: empty export_ids returns empty perspectives and loci", {
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE 1=0", stmt)) return(.empty_df)
    stop("Unexpected query with empty export_ids: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .persp_closure(character(0), conn = mc)
      expect_equal(nrow(result$perspectives), 0L)
      expect_equal(nrow(result$loci),         0L)
    }
  )
})

# ---------------------------------------------------------------------------
# .fetch_storage_rows
# ---------------------------------------------------------------------------

.ln_row <- function(id) {
  data.frame(id = as.character(id), Rack = "R1", Row = "A", BoxRow = "1",
             BoxColumn = "1", stringsAsFactors = FALSE)
}
.m80_row <- function(id) {
  data.frame(id = as.character(id), Drawer = "D1", Position = "1",
             BoxRow = "1", BoxColumn = "1", stringsAsFactors = FALSE)
}
test_that(".fetch_storage_rows: LiquidNitrogen rows for export_ids are returned", {
  mock <- function(stmt, conn = NULL) {
    if (grepl("LiquidNitrogen", stmt)) return(.ln_row("1"))
    if (grepl("Minus80Freezer", stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .storage_rows("1", conn = mc)
      expect_equal(nrow(result$liquid_nitrogen), 1L)
      expect_equal(result$liquid_nitrogen$id,    "1")
      expect_equal(nrow(result$minus80_freezer), 0L)
    }
  )
})

test_that(".fetch_storage_rows: Minus80Freezer rows for export_ids are returned", {
  mock <- function(stmt, conn = NULL) {
    if (grepl("LiquidNitrogen", stmt)) return(.empty_df)
    if (grepl("Minus80Freezer", stmt)) return(.m80_row("2"))
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .storage_rows("2", conn = mc)
      expect_equal(nrow(result$minus80_freezer), 1L)
      expect_equal(result$minus80_freezer$id,    "2")
    }
  )
})

test_that(".fetch_storage_rows: SQL IN clause contains only the supplied export_ids", {
  # Verify the queries target exactly the supplied ids — this is the mechanism
  # that excludes storage rows belonging to unrelated passaging ids.
  export_ids     <- c("10", "20")
  captured_stmts <- character(0)
  mock <- function(stmt, conn = NULL) {
    captured_stmts <<- c(captured_stmts, stmt)
    if (grepl("LiquidNitrogen", stmt)) return(.empty_df)
    if (grepl("Minus80Freezer", stmt)) return(.empty_df)
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      .storage_rows(export_ids, conn = mc)
      storage_stmts <- captured_stmts[grepl("WHERE.*IN", captured_stmts)]
      for (s in storage_stmts) {
        expect_match(s, "'10'")
        expect_match(s, "'20'")
        # An unrelated id must NOT appear.
        expect_false(grepl("'11'|'30'|'99'", s))
      }
    }
  )
})

test_that(".fetch_storage_rows: empty export_ids returns all three tables empty", {
  mock <- function(stmt, conn = NULL) {
    # Only WHERE 1=0 schema queries should be sent when export_ids is empty.
    if (grepl("WHERE 1=0", stmt)) return(.empty_df)
    stop("Unexpected non-schema query with empty export_ids: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- .storage_rows(character(0), conn = mc)
      expect_equal(nrow(result$liquid_nitrogen), 0L)
      expect_equal(nrow(result$minus80_freezer), 0L)
    }
  )
})

# ---------------------------------------------------------------------------
# export_passaging_subtree_bundle — format="rds" specific tests
# ---------------------------------------------------------------------------

test_that("format='rds': sql and statements are NULL", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_null(result$sql)
      expect_null(result$statements)
    }
  )
})

test_that("format='rds': bundle includes manifest_template and asset_inventory", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_true(is.character(result$manifest_template))
      expect_length(result$manifest_template, 1L)
      expect_true(is.list(result$asset_inventory))
      expect_named(result$asset_inventory, c("nodes", "imaging", "perspectives"))
    }
  )
})

test_that("format='rds': manifest_template is valid YAML seeded with exported nodes", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      manifest <- yaml::read_yaml(text = result$manifest_template)
      expect_equal(manifest$manifest_version, 1L)
      expect_equal(manifest$bundle_root_id, "1")
      expect_equal(manifest$request_email, "")
      expect_equal(manifest$options$package_format, "zip")
      expect_equal(manifest$options$layout, "by_node")
      expect_length(manifest$nodes, 1L)
      expect_equal(manifest$nodes[[1]]$passaging_id, "1")
      expect_equal(length(manifest$nodes[[1]]$perspectives), 0L)
      expect_equal(length(manifest$nodes[[1]]$imaging), 0L)
    }
  )
})

test_that("format='rds': asset_inventory summarizes perspectives even when tables$Perspective is empty", {
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =", stmt)) return(data.frame(id = "1", stringsAsFactors = FALSE))
    if (grepl("passaged_from_id1 =", stmt)) return(data.frame(id = character(0), stringsAsFactors = FALSE))
    if (grepl("SELECT \\* FROM `Passaging`", stmt)) return(.passaging_row("1"))
    if (grepl("WHERE 1=0|WHERE `name` IN|WHERE `id` IN \\(1\\)|MediaIngredients", stmt))
      return(data.frame())
    if (grepl("FROM `Perspective` WHERE `origin` IN", stmt) &&
        grepl("GROUP BY `origin`, `whichPerspective`", stmt)) {
      return(data.frame(
        passaging_id     = c("1", "1"),
        perspective_type = c("GenomePerspective", "TranscriptomePerspective"),
        row_count        = c(2L, 3L),
        stringsAsFactors = FALSE
      ))
    }
    if (grepl("Perspective WHERE `origin` IN", stmt))
      return(data.frame(cloneID = integer(0), origin = character(0),
                        profile_loci = integer(0), stringsAsFactors = FALSE))
    if (grepl("LiquidNitrogen|Minus80Freezer", stmt)) return(data.frame())
    stop("Unexpected query in perspective summary mock: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_equal(nrow(result$tables$Perspective), 0L)
      expect_equal(nrow(result$asset_inventory$perspectives), 2L)
      expect_equal(sort(result$asset_inventory$nodes$perspective_types[[1]]),
                   c("GenomePerspective", "TranscriptomePerspective"))
    }
  )
})

test_that("format='rds': imaging inventory uses anchored node-id prefix and leaks no raw paths", {
  root_dir <- tempfile("subtree_img_root_")
  img_dir <- file.path(root_dir, "Images")
  dir.create(img_dir, recursive = TRUE)
  on.exit(unlink(root_dir, recursive = TRUE), add = TRUE)
  file.create(file.path(img_dir, "abc_10x_ph_day1_overlay.png"))
  file.create(file.path(img_dir, "abcdef_10x_ph_day1_overlay.png"))
  file.create(file.path(img_dir, "prefix_abc_10x_ph_day1_overlay.png"))
  file.create(file.path(img_dir, "abc_result.csv"))

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =", stmt)) return(data.frame(id = "abc", stringsAsFactors = FALSE))
    if (grepl("passaged_from_id1 =", stmt)) return(data.frame(id = character(0), stringsAsFactors = FALSE))
    if (grepl("SELECT \\* FROM `Passaging`", stmt)) return(.passaging_row("abc"))
    if (grepl("WHERE 1=0|WHERE `name` IN|MediaIngredients", stmt)) return(data.frame())
    if (grepl("FROM `Perspective` WHERE `origin` IN", stmt) &&
        grepl("GROUP BY `origin`, `whichPerspective`", stmt))
      return(data.frame(passaging_id = character(0), perspective_type = character(0),
                        row_count = integer(0), stringsAsFactors = FALSE))
    if (grepl("Perspective WHERE `origin` IN", stmt))
      return(data.frame(cloneID = integer(0), origin = character(0),
                        profile_loci = integer(0), stringsAsFactors = FALSE))
    if (grepl("LiquidNitrogen|Minus80Freezer", stmt)) return(data.frame())
    stop("Unexpected query in imaging inventory mock: ", stmt)
  }

  with_mocked_bindings(
    .db_fetch = mock,
    .cellseg_paths = function() list(output = root_dir),
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "abc",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      inv <- result$asset_inventory$imaging
      expect_equal(nrow(inv), 1L)
      expect_equal(inv$asset_id, "img::abc::overlay")
      expect_equal(inv$file_count, 1L)
      expect_false(any(grepl("Images|/", capture.output(str(inv)))))
    }
  )
})

test_that("format='rds': metadata contains expected keys and correct values", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id        = "1",
        format    = "rds",
        recursive = TRUE,
        conn      = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      meta <- result$metadata
      expect_true(is.list(meta))
      expected_keys <- c("root_id", "format", "recursive", "include_storage",
                         "include_perspectives", "external_parent_policy",
                         "exported_at", "package_version")
      expect_true(all(expected_keys %in% names(meta)))
      expect_equal(meta$root_id,   "1")
      expect_equal(meta$format,    "rds")
      expect_equal(meta$recursive, TRUE)
    }
  )
})

test_that("format='rds': tables is a named list of data frames", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      tbls <- result$tables
      expect_true(is.list(tbls))
      for (nm in names(tbls)) {
        expect_true(is.data.frame(tbls[[nm]]),
                    info = paste0("tables$", nm, " should be a data.frame"))
      }
    }
  )
})

test_that("format='rds' with file= writes a readable RDS file", {
  mock <- .make_full_mock()
  tmp  <- tempfile(fileext = ".rds")
  on.exit(unlink(tmp), add = TRUE)
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        file   = tmp,
        format = "rds",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_true(file.exists(tmp))
      loaded <- readRDS(tmp)
      expect_equal(loaded$export_ids,        result$export_ids)
      expect_equal(loaded$metadata$format,   "rds")
      expect_equal(loaded$metadata$root_id,  "1")
    }
  )
})

test_that("format='sql' via export_passaging_subtree_bundle populates sql and statements", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1",
        format = "sql",
        conn   = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_false(is.null(result$sql))
      expect_false(is.null(result$statements))
      expect_equal(result$metadata$format, "sql")
    }
  )
})

test_that("export_passaging_subtree_sql wrapper returns same top-level structure as bundle(..., format='sql')", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      r_sql <- export_passaging_subtree_sql(
        id   = "1",
        conn = mc,
        include_storage      = FALSE,
        include_perspectives = FALSE
      )
      expect_named(r_sql, c("metadata", "export_ids", "detached_parent_refs",
                             "tables", "manifest_template", "asset_inventory",
                             "decoded", "sql", "statements"))
      expect_equal(r_sql$metadata$format, "sql")
    }
  )
})

# ---------------------------------------------------------------------------
# .decode_perspective_profiles
# ---------------------------------------------------------------------------
#
# All tests mock getSubProfiles() so no Java runtime or DB connection is needed.
# The helper does not accept a conn argument — decoding depends on the Java/
# CLONEID runtime, not an RMySQL object.
# ---------------------------------------------------------------------------

# Helper: build a minimal Perspective data frame row with all columns required
# by .decode_perspective_profiles().
.persp_row_full <- function(clone_id, origin,
                             which_p      = "GenomePerspective",
                             size         = 1,
                             parent       = NA_integer_,
                             profile_loci = NA_integer_) {
  data.frame(
    cloneID          = as.integer(clone_id),
    whichPerspective = as.character(which_p),
    origin           = as.character(origin),
    size             = as.numeric(size),
    parent           = as.integer(parent),
    profile_loci     = as.integer(profile_loci),
    profile          = I(list(as.raw(0x00))),
    stringsAsFactors = FALSE
  )
}

# Minimal numeric matrix as a stand-in for a decoded profile.
.mock_profile_matrix <- function() {
  m <- matrix(c(2, 2, 3, 2), nrow = 2L,
              dimnames = list(c("1:100-200", "2:300-400"),
                              c("ID1", "ID2")))
  m
}

test_that(".decode_perspective_profiles: empty data frame returns empty profiles and no warnings", {
  result <- .decode_profiles(data.frame())
  expect_equal(result$profiles,  list())
  expect_equal(result$warnings,  character(0))
})

test_that(".decode_perspective_profiles: NULL input returns empty profiles and no warnings", {
  result <- .decode_profiles(NULL)
  expect_equal(result$profiles,  list())
  expect_equal(result$warnings,  character(0))
})

test_that(".decode_perspective_profiles: missing required columns produces warning and empty profiles", {
  # data frame with no 'size' or 'whichPerspective'
  bad_df <- data.frame(cloneID = 1L, origin = "o1", stringsAsFactors = FALSE)
  result <- .decode_profiles(bad_df)
  expect_equal(result$profiles, list())
  expect_equal(length(result$warnings), 1L)
  expect_match(result$warnings[1], "missing required column")
})

test_that(".decode_perspective_profiles: valid single pair returns decoded matrix under correct keys", {
  # Two-node tree: root(42) -> child(43).  internal_ids = {42L}.
  df <- rbind(
    .persp_row_full(clone_id = 42L, origin = "origin1",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 43L, origin = "origin1",
                     which_p = "GenomePerspective", parent = 42L)
  )
  expected_mat <- .mock_profile_matrix()
  with_mocked_bindings(
    getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
      expect_equal(cloneID_or_sampleName, 42L)
      expect_equal(whichP, "GenomePerspective")
      expected_mat
    },
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      expect_equal(result$warnings, character(0))
      expect_true(!is.null(result$profiles[["GenomePerspective"]]))
      expect_true(!is.null(result$profiles[["GenomePerspective"]][["origin1"]]))
      expect_equal(result$profiles[["GenomePerspective"]][["origin1"]],
                   expected_mat)
    }
  )
})

test_that(".decode_perspective_profiles: leaf-only slice produces warning and skips pair", {
  # A single row with parent=NA: no row is an internal node; pair is skipped.
  df <- .persp_row_full(clone_id = 10L, origin = "o1")
  result <- .decode_profiles(df)
  expect_equal(result$profiles, list())
  expect_equal(length(result$warnings), 1L)
  expect_match(result$warnings[1], "no internal nodes")
})

test_that(".decode_perspective_profiles: multi-level tree decodes all internal nodes", {
  # Three-node tree: root(10) -> inner(11) -> leaf(12).
  # internal_ids = {10L, 11L}: getSubProfiles is called for each level.
  df <- rbind(
    .persp_row_full(clone_id = 10L, origin = "o1"),
    .persp_row_full(clone_id = 11L, origin = "o1", parent = 10L),
    .persp_row_full(clone_id = 12L, origin = "o1", parent = 11L)
  )
  mat_10 <- matrix(3, nrow = 1L, ncol = 1L,
                   dimnames = list("1:0-1", "ID11"))
  mat_11 <- matrix(4, nrow = 1L, ncol = 1L,
                   dimnames = list("1:0-1", "ID12"))
  with_mocked_bindings(
    getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
      if (cloneID_or_sampleName == 10L) return(mat_10)
      if (cloneID_or_sampleName == 11L) return(mat_11)
      stop("unexpected cloneID: ", cloneID_or_sampleName)
    },
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      expect_equal(result$warnings, character(0))
      combined <- result$profiles[["GenomePerspective"]][["o1"]]
      expect_false(is.null(combined))
      # Both levels decoded: columns from both getSubProfiles calls present.
      expect_true("ID11" %in% colnames(combined))
      expect_true("ID12" %in% colnames(combined))
      expect_equal(ncol(combined), 2L)
    }
  )
})

test_that(".decode_perspective_profiles: per-node error isolated; remaining nodes in same pair succeed", {
  # Three-node tree: root(30) -> inner(31) -> leaf(32).
  # internal_ids = {30L, 31L}.  getSubProfiles(30L) fails; (31L) succeeds.
  df <- rbind(
    .persp_row_full(clone_id = 30L, origin = "o1"),
    .persp_row_full(clone_id = 31L, origin = "o1", parent = 30L),
    .persp_row_full(clone_id = 32L, origin = "o1", parent = 31L)
  )
  mat_31 <- matrix(7, nrow = 1L, ncol = 1L, dimnames = list("1:0-1", "ID32"))
  with_mocked_bindings(
    getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
      if (cloneID_or_sampleName == 30L) stop("simulated error on root node")
      mat_31
    },
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      expect_equal(length(result$warnings), 1L)
      expect_match(result$warnings[1], "simulated error on root node")
      combined <- result$profiles[["GenomePerspective"]][["o1"]]
      expect_false(is.null(combined))
      expect_equal(colnames(combined), "ID32")
    }
  )
})

test_that(".decode_perspective_profiles: all per-node errors leave pair absent from profiles", {
  # Three-node tree; all getSubProfiles calls fail.
  df <- rbind(
    .persp_row_full(clone_id = 40L, origin = "o1"),
    .persp_row_full(clone_id = 41L, origin = "o1", parent = 40L),
    .persp_row_full(clone_id = 42L, origin = "o1", parent = 41L)
  )
  with_mocked_bindings(
    getSubProfiles = function(...) stop("always fails"),
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      expect_equal(length(result$warnings), 2L)   # one per internal node
      expect_null(result$profiles[["GenomePerspective"]])
    }
  )
})

test_that(".decode_perspective_profiles: getSubProfiles error records warning but preserves other pairs", {
  # Two origins; each has one internal node.  o1's node fails, o2's succeeds.
  df <- rbind(
    .persp_row_full(clone_id = 1L, origin = "o1",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 3L, origin = "o1",
                     which_p = "GenomePerspective", parent = 1L),
    .persp_row_full(clone_id = 2L, origin = "o2",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 4L, origin = "o2",
                     which_p = "GenomePerspective", parent = 2L)
  )
  expected_mat <- .mock_profile_matrix()
  with_mocked_bindings(
    getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
      if (cloneID_or_sampleName == 1L) stop("simulated Java error")
      expected_mat
    },
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      # o1 failed — warning recorded
      expect_equal(length(result$warnings), 1L)
      expect_match(result$warnings[1], "simulated Java error")
      # o2 succeeded — present in profiles
      expect_true(!is.null(result$profiles[["GenomePerspective"]][["o2"]]))
      expect_equal(result$profiles[["GenomePerspective"]][["o2"]], expected_mat)
      # o1 absent from profiles
      expect_null(result$profiles[["GenomePerspective"]][["o1"]])
    }
  )
})

# ---------------------------------------------------------------------------
# export_passaging_subtree_bundle — decode_profiles_recursive wiring
# ---------------------------------------------------------------------------
#
# getSubProfiles is mocked so no Java runtime or live DB is needed.
# The .make_full_mock() produces an empty Perspective table (no rows), so
# decoded is present but empty (list()) by default.  Tests that require
# Perspective rows use a bespoke mock.
#
# New semantics (replaces old decode_profiles param):
#   - format="rds" always runs shallow decode (decode_profiles_recursive=FALSE by default)
#   - format="sql" never decodes (decoded is NULL)
#   - decode_profiles_recursive=TRUE stops with an informative error (not yet implemented)
# ---------------------------------------------------------------------------

test_that("format='rds': bundle has 'decoded' slot (shallow decode by default)", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1", format = "rds", conn = mc,
        include_storage = FALSE, include_perspectives = FALSE
      )
      expect_true("decoded" %in% names(result))
    }
  )
})

test_that("format='sql': decoded is NULL and sql mode is unchanged", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1", format = "sql", conn = mc,
        include_storage = FALSE, include_perspectives = FALSE
      )
      expect_null(result$decoded)
      expect_false(is.null(result$sql))
      expect_false(is.null(result$statements))
    }
  )
})

test_that("format='rds': metadata contains decode_profiles_recursive=FALSE and decode_warnings", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1", format = "rds", conn = mc,
        include_storage = FALSE, include_perspectives = FALSE
      )
      meta <- result$metadata
      expect_true("decode_profiles_recursive" %in% names(meta))
      expect_true("decode_warnings"           %in% names(meta))
      expect_false(meta$decode_profiles_recursive)   # default is shallow (FALSE)
      expect_true(is.character(meta$decode_warnings))
    }
  )
})

test_that("format='sql': metadata contains decode_profiles_recursive=FALSE", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      result <- export_passaging_subtree_bundle(
        id     = "1", format = "sql", conn = mc,
        include_storage = FALSE, include_perspectives = FALSE
      )
      expect_false(result$metadata$decode_profiles_recursive)
    }
  )
})

test_that("decode_profiles_recursive=TRUE stops with not-yet-implemented error", {
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      expect_error(
        export_passaging_subtree_bundle(
          id = "1", format = "rds", conn = mc,
          decode_profiles_recursive = TRUE,
          include_storage = FALSE, include_perspectives = FALSE
        ),
        regexp = "not yet implemented"
      )
    }
  )
})

test_that("format='rds': tables$Perspective is unchanged by decoding", {
  # Perspective table should be the raw DB-faithful data frame regardless of decoding.
  # With new API both calls use shallow decode (decode_profiles_recursive=FALSE).
  mock <- .make_full_mock()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      r1 <- export_passaging_subtree_bundle(
        id = "1", format = "rds", conn = mc,
        include_storage = FALSE, include_perspectives = FALSE
      )
      r2 <- export_passaging_subtree_bundle(
        id = "1", format = "rds", conn = mc,
        decode_profiles_recursive = FALSE,
        include_storage = FALSE, include_perspectives = FALSE
      )
      # Raw table must be identical in both calls.
      expect_equal(r1$tables$Perspective, r2$tables$Perspective)
    }
  )
})

test_that("format='rds' with Perspective rows: decoded populated; getSubProfiles result stored correctly", {
  # Two-node tree: root(7) -> child(8).  internal_ids = {7L}.
  persp_df <- rbind(
    .persp_row_full(clone_id = 7L, origin = "o1",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 8L, origin = "o1",
                     which_p = "GenomePerspective", parent = 7L)
  )
  loci_df  <- data.frame()

  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =",              stmt)) return(data.frame(id = "1", stringsAsFactors = FALSE))
    if (grepl("passaged_from_id1 =",    stmt)) return(data.frame(id = character(0), stringsAsFactors = FALSE))
    if (grepl("SELECT \\* FROM `Passaging`", stmt)) return(.passaging_row("1"))
    if (grepl("WHERE 1=0|WHERE `name` IN|WHERE `id` IN \\(1\\)|MediaIngredients", stmt))
      return(data.frame())
    if (grepl("Perspective.*WHERE.*origin.*IN", stmt)) return(persp_df)
    if (grepl("Loci",                    stmt)) return(loci_df)
    if (grepl("LiquidNitrogen|Minus80Freezer", stmt)) return(data.frame())
    stop("Unexpected query in decode-test mock: ", stmt)
  }

  expected_mat <- .mock_profile_matrix()
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      with_mocked_bindings(
        getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
          expect_equal(cloneID_or_sampleName, 7L)
          expect_equal(whichP, "GenomePerspective")
          expected_mat
        },
        .package = "cloneid",
        code = {
          result <- export_passaging_subtree_bundle(
            id = "1", format = "rds", conn = mc,
            include_storage = FALSE, include_perspectives = TRUE
          )
          expect_false(is.null(result$decoded))
          expect_false(is.null(result$decoded[["GenomePerspective"]]))
          expect_equal(result$decoded[["GenomePerspective"]][["o1"]], expected_mat)
          expect_equal(result$metadata$decode_warnings, character(0))
          # Raw Perspective table still intact (two rows: root + child)
          expect_equal(nrow(result$tables$Perspective), 2L)
        }
      )
    }
  )
})

test_that("format='rds': getSubProfiles error is recorded in decode_warnings, export still completes", {
  # Two-node tree: root(5) -> child(6).  internal_ids = {5L}.
  persp_df <- rbind(
    .persp_row_full(clone_id = 5L, origin = "o1",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 6L, origin = "o1",
                     which_p = "GenomePerspective", parent = 5L)
  )
  mock <- function(stmt, conn = NULL) {
    if (grepl("WHERE id =",              stmt)) return(data.frame(id = "1", stringsAsFactors = FALSE))
    if (grepl("passaged_from_id1 =",    stmt)) return(data.frame(id = character(0), stringsAsFactors = FALSE))
    if (grepl("SELECT \\* FROM `Passaging`", stmt)) return(.passaging_row("1"))
    if (grepl("WHERE 1=0|WHERE `name` IN|WHERE `id` IN \\(1\\)|MediaIngredients", stmt))
      return(data.frame())
    if (grepl("Perspective.*WHERE.*origin.*IN", stmt)) return(persp_df)
    if (grepl("Loci",                    stmt)) return(data.frame())
    if (grepl("LiquidNitrogen|Minus80Freezer", stmt)) return(data.frame())
    stop("Unexpected: ", stmt)
  }
  with_mocked_bindings(
    .db_fetch = mock,
    .package  = "cloneid",
    code = {
      with_mocked_bindings(
        getSubProfiles = function(...) stop("Java not available"),
        .package = "cloneid",
        code = {
          result <- export_passaging_subtree_bundle(
            id = "1", format = "rds", conn = mc,
            include_storage = FALSE, include_perspectives = TRUE
          )
          # Export completed despite decode failure
          expect_equal(result$export_ids, "1")
          # Warning recorded in metadata
          expect_true(length(result$metadata$decode_warnings) > 0L)
          expect_match(result$metadata$decode_warnings[1], "Java not available")
          # decoded slot is present but the failed pair is absent (partial result)
          expect_false(is.null(result$decoded))
        }
      )
    }
  )
})

test_that(".decode_perspective_profiles: two perspective types decoded independently", {
  # Each perspective type has its own two-node tree (root -> child).
  df <- rbind(
    .persp_row_full(clone_id = 1L, origin = "o1",
                     which_p = "GenomePerspective"),
    .persp_row_full(clone_id = 3L, origin = "o1",
                     which_p = "GenomePerspective",       parent = 1L),
    .persp_row_full(clone_id = 2L, origin = "o1",
                     which_p = "TranscriptomePerspective"),
    .persp_row_full(clone_id = 4L, origin = "o1",
                     which_p = "TranscriptomePerspective", parent = 2L)
  )
  mat_g <- .mock_profile_matrix()
  mat_t <- .mock_profile_matrix() * 2
  with_mocked_bindings(
    getSubProfiles = function(cloneID_or_sampleName, whichP, ...) {
      if (whichP == "GenomePerspective")        return(mat_g)
      if (whichP == "TranscriptomePerspective") return(mat_t)
      stop("unexpected whichP: ", whichP)
    },
    .package = "cloneid",
    code = {
      result <- .decode_profiles(df)
      expect_equal(result$warnings, character(0))
      expect_equal(result$profiles[["GenomePerspective"]][["o1"]],        mat_g)
      expect_equal(result$profiles[["TranscriptomePerspective"]][["o1"]], mat_t)
    }
  )
})
