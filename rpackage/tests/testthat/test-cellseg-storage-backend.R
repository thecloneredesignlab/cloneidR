library(testthat)

.cfg     <- cloneid:::.cellseg_read_config
.paths   <- cloneid:::.cellseg_paths
.is_s3   <- cloneid:::.cellseg_is_s3
.is_local<- cloneid:::.cellseg_is_local
.deletep <- cloneid:::.cellseg_delete_paths
.copy_in <- cloneid:::.cellseg_copy_to_input
.copy_out<- cloneid:::.cellseg_copy_to_output
.subs    <- cloneid:::.cellseg_output_subdirs

test_that(".cellseg_read_config defaults backend to local when absent", {
  with_mocked_bindings(
    .read_yaml = NULL,
    .package = "cloneid",
    code = {
      mock_cfg <- list(cellSegmentation = list(
        input = "/tmp/input",
        output = "/tmp/output",
        tmp = "/tmp/tmp"
      ))

      result <- with_mocked_bindings(
        read_yaml = function(...) mock_cfg,
        .package = "yaml",
        code = .cfg()
      )

      expect_identical(result$backend, "local")
      expect_identical(result$input, "/tmp/input")
      expect_identical(result$output, "/tmp/output")
      expect_identical(result$tmp, "/tmp/tmp")
    }
  )
})

test_that(".cellseg_is_local and .cellseg_is_s3 reflect backend selection", {
  expect_true(.is_local(list(backend = "local")))
  expect_false(.is_s3(list(backend = "local")))
  expect_true(.is_s3(list(backend = "s3")))
  expect_false(.is_local(list(backend = "s3")))
})

test_that(".cellseg_paths returns normalized local paths and rejects s3 backend", {
  local_cfg <- list(
    backend = "local",
    input = tempdir(),
    output = tempdir(),
    tmp = tempdir()
  )

  local_paths <- with_mocked_bindings(
    .cellseg_read_config = function() local_cfg,
    .package = "cloneid",
    code = .paths()
  )

  expect_match(local_paths$input, "/$")
  expect_match(local_paths$output, "/$")
  expect_true(nzchar(local_paths$tmp))

  expect_error(
    with_mocked_bindings(
      .cellseg_read_config = function() list(backend = "s3"),
      .package = "cloneid",
      code = .paths()
    ),
    "S3 cellSegmentation backend is configured"
  )
})

test_that(".cellseg_delete_paths removes only matching id-scoped artifacts", {
  root <- tempfile("cellseg-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  dir.create(indir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  keep_in <- file.path(indir, "OTHER_10x_ph_bl.tif")
  rm_in <- file.path(indir, "CASE123_10x_ph_bl.tif")
  file.create(keep_in)
  file.create(rm_in)

  keep_out <- file.path(outdir, "Images", "OTHER_overlay.png")
  rm_out <- file.path(outdir, "Images", "CASE123_overlay.png")
  file.create(keep_out)
  file.create(rm_out)

  .deletep("CASE123", indir, outdir)

  expect_true(file.exists(keep_in))
  expect_false(file.exists(rm_in))
  expect_true(file.exists(keep_out))
  expect_false(file.exists(rm_out))
})

test_that(".cellseg_copy_to_input and .cellseg_copy_to_output copy files in local mode", {
  root <- tempfile("cellseg-copy-")
  dir.create(root)
  indir <- file.path(root, "input")
  outdir <- file.path(root, "output")
  tmpdir <- file.path(root, "tmp")
  dir.create(indir)
  dir.create(outdir)
  dir.create(tmpdir)
  for (sub in .subs()) dir.create(file.path(outdir, sub), recursive = TRUE)

  src_file <- file.path(root, "CASE123_10x_ph_bl.tif")
  writeLines("x", src_file)

  mock_cfg <- list(
    backend = "local",
    input = indir,
    output = outdir,
    tmp = tmpdir
  )

  with_mocked_bindings(
    .cellseg_read_config = function() mock_cfg,
    .package = "cloneid",
    code = {
      .copy_in(src_file)
      .copy_out(src_file, "Images")
    }
  )

  expect_true(file.exists(file.path(indir, basename(src_file))))
  expect_true(file.exists(file.path(outdir, "Images", basename(src_file))))
})
