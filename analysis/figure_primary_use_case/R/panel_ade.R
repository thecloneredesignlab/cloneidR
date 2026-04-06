panel_a_root_id <- "SNU-668_K3_A24_seedT4"

panel_de_selected_nodes <- c(
  "SNU-668_C_A24_seed",
  "SNU-668_C_A4_seed",
  "SNU-668_K1_A34_seedT1",
  "SNU-668_K2_A34_seedT1",
  "SNU-668_K3_A34_seedT1",
  "SNU-668_r1_A55_seedT1",
  "SNU-668_r2_A55_seedT1",
  "SNU-668_r3_A55_seedT1"
)

panel_de_lineage_colors <- c(
  "SNU-668_C_A24_seed" = "#8c6d31",
  "SNU-668_C_A4_seed" = "#c2a15a",
  "SNU-668_K1_A34_seedT1" = "#355f8d",
  "SNU-668_K2_A34_seedT1" = "#4f86b8",
  "SNU-668_K3_A34_seedT1" = "#79acd8",
  "SNU-668_r1_A55_seedT1" = "#8a3b12",
  "SNU-668_r2_A55_seedT1" = "#bf5b17",
  "SNU-668_r3_A55_seedT1" = "#e08214"
)

derive_panel_a_manifest <- function(subtree_rds,
                                    root_id = panel_a_root_id,
                                    output_yaml) {
  x <- read_subtree_bundle(subtree_rds)
  p <- x[["tables"]][["Passaging"]]
  ids <- descendant_ids(p, root_id)
  manifest <- read_bundle_manifest_template(subtree_rds)
  node_lookup <- setNames(manifest$nodes, vapply(manifest$nodes, `[[`, character(1), "passaging_id"))
  manifest$bundle_root_id <- root_id
  manifest$nodes <- unname(node_lookup[ids])
  manifest$nodes <- lapply(manifest$nodes, function(node) {
    node$perspectives <- empty_yaml_sequence()
    node$imaging <- normalize_yaml_sequence(grep("::images$", node$imaging, value = TRUE))
    node
  })
  
  write_manifest_if_changed(manifest, output_yaml)
  
  data.frame(
    passaging_id = ids,
    imaging_asset = sprintf("img::%s::images", ids),
    stringsAsFactors = FALSE
  )
}

derive_panel_de_manifest <- function(subtree_rds,
                                     output_yaml,
                                     bundle_root_id = "SNU-668_dp_A9_seedT2",
                                     selected_nodes = panel_de_selected_nodes) {
  x <- read_subtree_bundle(subtree_rds)
  ai <- x[["asset_inventory"]][["perspectives"]]
  if (is.null(ai) || nrow(ai) == 0) {
    stop("No perspective assets were advertised in the subtree bundle.")
  }
  ai <- ai[ai$perspective_type == "GenomePerspective", , drop = FALSE]
  if (nrow(ai) == 0) {
    stop("No GenomePerspective assets were advertised in the subtree bundle.")
  }
  ai <- ai[ai$passaging_id %in% selected_nodes, , drop = FALSE]
  if (nrow(ai) == 0) {
    stop("None of the selected panel D/E GenomePerspective nodes were advertised in the subtree bundle.")
  }
  
  ids <- selected_nodes[selected_nodes %in% unique(ai$passaging_id)]
  manifest <- read_bundle_manifest_template(subtree_rds)
  node_lookup <- setNames(manifest$nodes, vapply(manifest$nodes, `[[`, character(1), "passaging_id"))
  manifest$bundle_root_id <- bundle_root_id
  manifest$nodes <- unname(node_lookup[ids])
  manifest$nodes <- lapply(manifest$nodes, function(node) {
    node$perspectives <- normalize_yaml_sequence(sort(unique(ai$perspective_type[ai$passaging_id == node$passaging_id])))
    node$imaging <- empty_yaml_sequence()
    node
  })
  
  write_manifest_if_changed(manifest, output_yaml)
  
  data.frame(
    passaging_id = ids,
    perspective_types = vapply(
      ids,
      function(id) paste(sort(unique(ai$perspective_type[ai$passaging_id == id])), collapse = ", "),
      character(1)
    ),
    stringsAsFactors = FALSE
  )
}

manual_portal_steps <- function(paths) {
  data.frame(
    step = c(
      "If subtree RDS is missing",
      "For panel A",
      "For panels D/E"
    ),
    action = c(
      "Download subtree_SNU-668_dp_A9_seedT2.rds from the portal subtree export for SNU-668_dp_A9_seedT2 and place it in input/subtree/.",
      paste(
        "Render this Rmd to generate input/manifest_A/manifest_panel_A.yaml,",
        "upload that manifest in the portal, then copy the resulting panelA.zip into input/subtree/."
      ),
      paste(
        "Render this Rmd to generate input/manifest_DE/manifest_panel_DE.yaml,",
        "upload that manifest in the portal, then copy the resulting panelDE.zip into input/subtree/."
      )
    ),
    stringsAsFactors = FALSE
  )
}

panel_a_status <- function(manifest_a_yaml, panel_a_zip) {
  data.frame(
    panel = "A",
    status = if (file.exists(manifest_a_yaml) && file.exists(panel_a_zip)) "download_ready_for_render" else if (file.exists(manifest_a_yaml)) "manifest_ready_awaiting_downloaded_export" else "awaiting_manifest_generation",
    note = paste(
      "Panel A requires the curated manifest rooted at SNU-668_K3_A24_seedT4",
      "and the downloaded overlay export zip."
    ),
    stringsAsFactors = FALSE
  )
}

panel_de_status <- function(manifest_de_yaml, panel_de_zip) {
  data.frame(
    panel = "D/E",
    status = if (file.exists(manifest_de_yaml) && file.exists(panel_de_zip)) "download_ready_for_render" else if (file.exists(manifest_de_yaml)) "manifest_ready_awaiting_downloaded_export" else "awaiting_manifest_generation",
    note = paste(
      "Panels D and E require the whole-subtree manifest download for the selected",
      "eight GenomePerspective nodes (six late test lineages plus two controls),",
      "supplied as panelDE.zip."
    ),
    stringsAsFactors = FALSE
  )
}

final_assembly_status <- function(output_panels_dir, output_final_dir) {
  final_png <- file.path(output_final_dir, "figure_primary_use_case_panels.png")
  status <- if (file.exists(final_png)) "rendered" else "awaiting_render"
  data.frame(
    stage = "final_assembly",
    panels_dir = output_panels_dir,
    final_dir = output_final_dir,
    final_png = final_png,
    status = status,
    stringsAsFactors = FALSE
  )
}

read_panel_image <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("Expected panel image not found: %s", path))
  }
  magick::image_read(path)
}

trim_panel_image <- function(img, fuzz = 8) {
  magick::image_trim(img, fuzz = fuzz)
}

resize_to_height <- function(img, height_px) {
  info <- magick::image_info(img)
  width_px <- max(1L, as.integer(round(info$width[[1]] * height_px / info$height[[1]])))
  magick::image_resize(img, sprintf("%dx%d!", width_px, height_px))
}

resize_to_width <- function(img, width_px) {
  info <- magick::image_info(img)
  height_px <- max(1L, as.integer(round(info$height[[1]] * width_px / info$width[[1]])))
  magick::image_resize(img, sprintf("%dx%d!", width_px, height_px))
}

label_panel_image <- function(img, label, pointsize = NULL, x_offset = NULL, y_offset = NULL) {
  info <- magick::image_info(img)
  if (is.null(pointsize)) {
    pointsize <- as.integer(round(
      min(
        cloneid_figure_text_sizes$panel_label_max_pointsize,
        max(
          cloneid_figure_text_sizes$panel_label_min_pointsize,
          min(info$width[[1]], info$height[[1]]) * cloneid_figure_text_sizes$panel_label_size_frac
        )
      )
    ))
  }
  if (is.null(x_offset)) {
    x_offset <- as.integer(round(info$width[[1]] * cloneid_figure_text_sizes$panel_label_x_offset_frac))
  }
  if (is.null(y_offset)) {
    y_offset <- as.integer(round(info$height[[1]] * cloneid_figure_text_sizes$panel_label_y_offset_frac))
  }
  magick::image_annotate(
    img,
    text = label,
    gravity = "northwest",
    location = sprintf("+%d+%d", x_offset, y_offset),
    size = pointsize,
    weight = 700,
    color = "black"
  )
}

prepare_assembled_panel_image <- function(path, max_width = 1000) {
  img <- read_panel_image(path)
  img <- trim_panel_image(img)
  img <- magick::image_border(img, color = "white", geometry = "24x24")
  info <- magick::image_info(img)
  if (info$width[[1]] > max_width) {
    img <- resize_to_width(img, max_width)
  }
  img
}

stack_images_vertical <- function(images) {
  widths <- vapply(images, function(img) magick::image_info(img)$width[[1]], integer(1))
  target_width <- max(widths)
  normalized <- lapply(images, function(img) resize_to_width(img, target_width))
  magick::image_append(do.call(c, normalized), stack = TRUE)
}

stack_images_horizontal <- function(images) {
  heights <- vapply(images, function(img) magick::image_info(img)$height[[1]], integer(1))
  target_height <- max(heights)
  normalized <- lapply(images, function(img) resize_to_height(img, target_height))
  magick::image_append(do.call(c, normalized), stack = FALSE)
}

build_primary_use_case_right_block <- function(output_panels_dir,
                                               subtree_rds,
                                               panel_a_zip,
                                               cache_dir) {
  panel_paths <- list(
    B = file.path(output_panels_dir, "panel_B_growth_curves.png"),
    C = file.path(output_panels_dir, "panel_C_trends.png"),
    D = file.path(output_panels_dir, "panel_D_ploidy_surrogate.png"),
    E = file.path(output_panels_dir, "panel_E_genome_heatmap.png")
  )
  
  panel_images <- lapply(names(panel_paths), function(label) {
    max_width <- if (identical(label, "B")) 1000 else 900
    prepare_assembled_panel_image(panel_paths[[label]], max_width = max_width)
  })
  names(panel_images) <- names(panel_paths)
  c_block <- panel_images$C
  bottom_row <- stack_images_vertical(list(panel_images$D, panel_images$E))
  
  row_width <- max(
    magick::image_info(panel_images$B)$width[[1]],
    magick::image_info(c_block)$width[[1]],
    magick::image_info(bottom_row)$width[[1]]
  )
  
  top_row <- resize_to_width(panel_images$B, row_width)
  # top_row <- label_panel_image(top_row, "C")
  # c_block <- label_panel_image(resize_to_width(c_block, row_width), "D")
  d_panel <- resize_to_width(panel_images$D, row_width)
  d_info <- magick::image_info(d_panel)
  # d_panel <- label_panel_image(d_panel, "E",y_offset = as.integer(round(d_info$height[[1]] * -0.15)))
  e_panel <- label_panel_image(resize_to_width(panel_images$E, row_width), "F")
  bottom_row <- stack_images_vertical(list(d_panel, e_panel))
  
  stack_images_vertical(list(top_row, c_block, bottom_row))
}

assemble_primary_use_case_figure <- function(output_panels_dir,
                                             output_final_dir,
                                             subtree_rds,
                                             panel_a_zip,
                                             cache_dir) {
  dir.create(output_final_dir, recursive = TRUE, showWarnings = FALSE)
  right_block <- build_primary_use_case_right_block(
    output_panels_dir = output_panels_dir,
    subtree_rds = subtree_rds,
    panel_a_zip = panel_a_zip,
    cache_dir = cache_dir
  )
  right_info <- magick::image_info(right_block)
  
  panel_a0_path <- tempfile(fileext = ".png")
  render_panel_a0_png(subtree_rds = subtree_rds, output_path = panel_a0_path)
  panel_a0 <- prepare_assembled_panel_image(panel_a0_path, max_width = 700)
  
  panel_a_tree_path <- tempfile(fileext = ".png")
  render_panel_a_tree_only_png(
    subtree_rds = subtree_rds,
    panel_a_zip = panel_a_zip,
    cache_dir = cache_dir,
    output_path = panel_a_tree_path
  )
  panel_a_tree <- prepare_assembled_panel_image(panel_a_tree_path, max_width = 700)
  panel_a_legend <- trim_panel_image(build_panel_a_legend_image(subtree_rds))
  panel_a_legend <- magick::image_border(panel_a_legend, color = "white", geometry = "12x12")
  panel_a <- stack_images_horizontal(list(panel_a_tree, panel_a_legend))
  
  target_left_width <- max(
    magick::image_info(panel_a0)$width[[1]],
    magick::image_info(panel_a)$width[[1]]
  )
  panel_a0 <- resize_to_width(panel_a0, round(target_left_width*1.2))
  panel_a <- resize_to_width(panel_a, target_left_width)
  
  target_a0_height <- max(1L, as.integer(round(right_info$height[[1]] * 0.9)))
  target_a_height <- max(1L, right_info$height[[1]] - target_a0_height)
  panel_a0 <- resize_to_height(panel_a0, target_a0_height)
  panel_a <- resize_to_height(panel_a, target_a_height)
  # panel_a0 <- label_panel_image(panel_a0, "A")
  # panel_a <- label_panel_image(panel_a, "B")
  left_block <- stack_images_vertical(list(panel_a0, panel_a))
  left_info <- magick::image_info(left_block)
  
  assembled <- stack_images_horizontal(list(left_block, right_block))
  assembled_info <- magick::image_info(assembled)
  output_png <- file.path(output_final_dir, "figure_primary_use_case_panels.png")
  output_meta <- file.path(output_final_dir, "figure_primary_use_case_panels_meta.csv")
  magick::image_write(assembled, path = output_png, format = "png")
  utils::write.csv(
    data.frame(
      final_width = assembled_info$width[[1]],
      final_height = assembled_info$height[[1]],
      right_block_width = right_info$width[[1]],
      right_block_height = right_info$height[[1]],
      left_block_width = left_info$width[[1]],
      left_block_height = left_info$height[[1]],
      extra_width_ratio = (assembled_info$width[[1]] - right_info$width[[1]]) / right_info$width[[1]],
      stringsAsFactors = FALSE
    ),
    output_meta,
    row.names = FALSE
  )
  
  list(
    output_png = output_png,
    files = data.frame(
      artifact = c("assembled_figure_png", "assembled_figure_meta"),
      path = c(output_png, output_meta),
      stringsAsFactors = FALSE
    ),
    image = assembled
  )
}

verify_assembled_figure_contains_a0 <- function(output_meta,
                                                min_extra_width_ratio = 0.2) {
  meta <- utils::read.csv(output_meta, stringsAsFactors = FALSE)
  meta$a0_present <- meta$extra_width_ratio >= min_extra_width_ratio
  meta
}

find_panel_a_zip <- function(root_dir = ".", panel_a_zip = NULL) {
  zip_path <- if (is.null(panel_a_zip)) file.path(root_dir, "input", "subtree", "panelA.zip") else panel_a_zip
  if (!file.exists(zip_path)) {
    stop(sprintf("Panel A zip not found at '%s'.", zip_path))
  }
  normalizePath(zip_path, winslash = "/", mustWork = TRUE)
}

list_panel_a_members <- function(panel_a_zip) {
  listing <- utils::unzip(panel_a_zip, list = TRUE)
  members <- listing$Name
  members[grepl("^imaging/.+/images/.+_mask_overlay\\.tif$", members)]
}

first_panel_a_overlay_members <- function(panel_a_zip, node_ids) {
  members <- sort(list_panel_a_members(panel_a_zip))
  chosen <- vapply(node_ids, function(node_id) {
    hits <- members[grepl(sprintf("^imaging/%s/images/", node_id), members)]
    if (length(hits) == 0) {
      return(NA_character_)
    }
    hits[[1]]
  }, character(1))
  
  data.frame(
    passaging_id = node_ids,
    zip_member = unname(chosen),
    stringsAsFactors = FALSE
  )
}

extract_panel_a_overlays <- function(panel_a_zip, overlay_map, cache_dir) {
  out_dir <- file.path(cache_dir, "panelA_overlays")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  valid <- overlay_map[!is.na(overlay_map$zip_member), , drop = FALSE]
  if (nrow(valid) == 0) {
    return(setNames(character(0), character(0)))
  }
  
  utils::unzip(panel_a_zip, files = valid$zip_member, exdir = out_dir)
  extracted <- file.path(out_dir, valid$zip_member)
  names(extracted) <- valid$passaging_id
  extracted
}

crop_overlay_to_png <- function(input_path, output_path, crop_width_frac = 0.2, crop_height_frac = 0.2, resize_px = 180) {
  img <- magick::image_read(input_path)
  info <- magick::image_info(img)
  crop_w <- max(1L, as.integer(round(info$width[[1]] * crop_width_frac)))
  crop_h <- max(1L, as.integer(round(info$height[[1]] * crop_height_frac)))
  max_offset_x <- max(0L, info$width[[1]] - crop_w)
  max_offset_y <- max(0L, info$height[[1]] - crop_h)
  offset_x <- if (max_offset_x > 0L) sample.int(max_offset_x + 1L, 1L) - 1L else 0L
  offset_y <- if (max_offset_y > 0L) sample.int(max_offset_y + 1L, 1L) - 1L else 0L
  geom <- sprintf("%dx%d+%d+%d", crop_w, crop_h, offset_x, offset_y)
  cropped <- magick::image_crop(img, geom)
  resized <- magick::image_resize(cropped, sprintf("%dx%d!", resize_px, resize_px))
  bordered <- magick::image_border(resized, color = "white", geometry = "8x8")
  magick::image_write(bordered, path = output_path, format = "png")
  output_path
}

prepare_panel_a_rasters <- function(extracted_paths, cache_dir) {
  out_dir <- file.path(cache_dir, "panelA_png")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  rasters <- lapply(names(extracted_paths), function(node_id) {
    png_path <- file.path(out_dir, sprintf("%s.png", node_id))
    crop_overlay_to_png(extracted_paths[[node_id]], png_path)
    png::readPNG(png_path)
  })
  names(rasters) <- names(extracted_paths)
  rasters
}

panel_a_tree_layout <- function(passaging, root_id = panel_a_root_id) {
  sub_ids <- descendant_ids(passaging, root_id)
  p <- passaging[passaging$id %in% sub_ids, , drop = FALSE]
  p <- p[order(p$passage, p$id), , drop = FALSE]
  children <- split(p$id, p$passaged_from_id1)
  children <- lapply(children, sort)
  
  angle_map <- list()
  leaf_order <- character()
  
  assign_leaf_angles <- function(node_id) {
    kids <- children[[node_id]]
    if (is.null(kids) || length(kids) == 0) {
      leaf_order <<- c(leaf_order, node_id)
      return(node_id)
    }
    unlist(lapply(kids, assign_leaf_angles), use.names = FALSE)
  }
  
  leaves <- assign_leaf_angles(root_id)
  leaf_angles <- seq(0, 2 * pi, length.out = length(leaf_order) + 1)[seq_along(leaf_order)]
  names(leaf_angles) <- leaf_order
  
  node_angle <- function(node_id) {
    kids <- children[[node_id]]
    if (is.null(kids) || length(kids) == 0) {
      return(leaf_angles[[node_id]])
    }
    mean(vapply(kids, node_angle, numeric(1)))
  }
  
  node_ids <- sub_ids
  angles <- vapply(node_ids, node_angle, numeric(1))
  names(angles) <- node_ids
  
  depths <- setNames(p$passage - min(p$passage, na.rm = TRUE), p$id)
  max_depth <- max(depths, na.rm = TRUE)
  radius <- if (max_depth == 0) rep(1, length(depths)) else 0.2 + 0.8 * (depths / max_depth)
  names(radius) <- names(depths)
  
  nodes <- data.frame(
    id = node_ids,
    angle = unname(angles[node_ids]),
    radius = unname(radius[node_ids]),
    x = unname(radius[node_ids] * cos(angles[node_ids])),
    y = unname(radius[node_ids] * sin(angles[node_ids])),
    passage = p$passage[match(node_ids, p$id)],
    stringsAsFactors = FALSE
  )
  
  edge_idx <- !is.na(p$passaged_from_id1) & p$id != root_id
  edges <- data.frame(
    parent = p$passaged_from_id1[edge_idx],
    child = p$id[edge_idx],
    child_passage = p$passage[edge_idx],
    stringsAsFactors = FALSE
  )
  edges$x0 <- nodes$x[match(edges$parent, nodes$id)]
  edges$y0 <- nodes$y[match(edges$parent, nodes$id)]
  edges$x1 <- nodes$x[match(edges$child, nodes$id)]
  edges$y1 <- nodes$y[match(edges$child, nodes$id)]
  
  list(nodes = nodes, edges = edges)
}

panel_a_subtree_to_phylo <- function(passaging, root_id = panel_a_root_id) {
  sub_ids <- descendant_ids(passaging, root_id)
  p <- passaging[passaging$id %in% sub_ids, , drop = FALSE]
  p <- p[order(p$passage, p$id), , drop = FALSE]
  children <- split(p$id, p$passaged_from_id1)
  children <- lapply(children, sort)
  
  tip_labels <- character()
  internal_ids <- character()
  
  assign_order <- function(node_id) {
    kids <- children[[node_id]]
    if (is.null(kids) || length(kids) == 0) {
      tip_labels <<- c(tip_labels, node_id)
      return(invisible(NULL))
    }
    internal_ids <<- c(internal_ids, node_id)
    invisible(lapply(kids, assign_order))
  }
  
  assign_order(root_id)
  internal_ids <- unique(internal_ids)
  
  tip_index <- setNames(seq_along(tip_labels), tip_labels)
  internal_index <- setNames(length(tip_labels) + seq_along(internal_ids), internal_ids)
  
  edge_rows <- list()
  edge_passages <- integer()
  
  for (parent_id in internal_ids) {
    kids <- children[[parent_id]]
    if (is.null(kids) || length(kids) == 0) {
      next
    }
    parent_idx <- internal_index[[parent_id]]
    for (child_id in kids) {
      child_idx <- if (child_id %in% tip_labels) tip_index[[child_id]] else internal_index[[child_id]]
      edge_rows[[length(edge_rows) + 1L]] <- c(parent_idx, child_idx)
      edge_passages[[length(edge_passages) + 1L]] <- p$passage[match(child_id, p$id)]
    }
  }
  
  phy <- structure(
    list(
      edge = do.call(rbind, edge_rows),
      tip.label = tip_labels,
      Nnode = length(internal_ids)
    ),
    class = "phylo"
  )
  phy <- ape::reorder.phylo(phy, order = "cladewise")
  
  edge_child_ids <- c(
    phy$tip.label[phy$edge[, 2] <= length(phy$tip.label)],
    internal_ids[match(
      phy$edge[, 2][phy$edge[, 2] > length(phy$tip.label)] - length(phy$tip.label),
      seq_along(internal_ids)
    )]
  )
  edge_child_ids <- vapply(
    phy$edge[, 2],
    function(idx) {
      if (idx <= length(phy$tip.label)) {
        phy$tip.label[[idx]]
      } else {
        internal_ids[[idx - length(phy$tip.label)]]
      }
    },
    character(1)
  )
  edge_passages <- p$passage[match(edge_child_ids, p$id)]
  
  list(
    phy = phy,
    passaging = p,
    tip_labels = tip_labels,
    edge_child_ids = edge_child_ids,
    edge_passages = edge_passages
  )
}

full_subtree_tree_layout <- function(passaging, root_id = "SNU-668_rK_A0_seed") {
  sub_ids <- descendant_ids(passaging, root_id)
  p <- passaging[passaging$id %in% sub_ids, , drop = FALSE]
  p <- p[order(p$id), , drop = FALSE]
  children <- split(p$id, p$passaged_from_id1)
  children <- lapply(children, sort)
  
  leaf_order <- character()
  assign_leaf_order <- function(node_id) {
    kids <- children[[node_id]]
    if (is.null(kids) || length(kids) == 0) {
      leaf_order <<- c(leaf_order, node_id)
      return(invisible(NULL))
    }
    invisible(lapply(kids, assign_leaf_order))
  }
  assign_leaf_order(root_id)
  
  leaf_y <- seq(0, 1, length.out = max(1L, length(leaf_order)))
  names(leaf_y) <- leaf_order
  
  node_y <- function(node_id) {
    kids <- children[[node_id]]
    if (is.null(kids) || length(kids) == 0) {
      return(leaf_y[[node_id]])
    }
    mean(vapply(kids, node_y, numeric(1)))
  }
  
  node_depths <- setNames(rep(NA_integer_, length(sub_ids)), sub_ids)
  node_depths[[root_id]] <- 0L
  queue <- root_id
  while (length(queue) > 0) {
    cur <- queue[[1]]
    queue <- queue[-1]
    kids <- children[[cur]]
    if (is.null(kids) || length(kids) == 0) {
      next
    }
    for (kid in kids) {
      node_depths[[kid]] <- node_depths[[cur]] + 1L
      queue <- c(queue, kid)
    }
  }
  
  max_depth <- max(node_depths, na.rm = TRUE)
  node_x <- if (max_depth == 0) rep(0, length(node_depths)) else node_depths / max_depth
  names(node_x) <- names(node_depths)
  
  nodes <- data.frame(
    id = sub_ids,
    x = unname(node_x[sub_ids]),
    y = vapply(sub_ids, node_y, numeric(1)),
    stringsAsFactors = FALSE
  )
  
  edge_idx <- !is.na(p$passaged_from_id1) & p$id != root_id
  edges <- data.frame(
    parent = p$passaged_from_id1[edge_idx],
    child = p$id[edge_idx],
    regime = vapply(p$id[edge_idx], selection_regime_from_id, character(1)),
    stringsAsFactors = FALSE
  )
  edges$x0 <- nodes$x[match(edges$parent, nodes$id)]
  edges$y0 <- nodes$y[match(edges$parent, nodes$id)]
  edges$x1 <- nodes$x[match(edges$child, nodes$id)]
  edges$y1 <- nodes$y[match(edges$child, nodes$id)]
  
  list(nodes = nodes, edges = edges)
}

plot_panel_a <- function(subtree_rds,
                         panel_a_zip,
                         cache_dir,
                         root_id = panel_a_root_id,
                         include_legend = TRUE) {
  x <- read_subtree_bundle(subtree_rds)
  passaging <- x[["tables"]][["Passaging"]]
  phylo_data <- panel_a_subtree_to_phylo(passaging, root_id = root_id)
  overlay_map <- first_panel_a_overlay_members(panel_a_zip, phylo_data$tip_labels)
  extracted <- extract_panel_a_overlays(panel_a_zip, overlay_map, cache_dir)
  rasters <- prepare_panel_a_rasters(extracted, cache_dir)
  
  passage_levels <- sort(unique(phylo_data$edge_passages))
  passage_palette <- grDevices::colorRampPalette(
    c("#d8d2c4", "#c58f2f", "#6f8f3a", "#28536b")
  )(length(passage_levels))
  names(passage_palette) <- as.character(passage_levels)
  edge_col <- unname(passage_palette[as.character(phylo_data$edge_passages)])
  
  op <- graphics::par(mar = c(0, 0, 0, 0), xpd = NA)
  on.exit(graphics::par(op), add = TRUE)
  ape::plot.phylo(
    phylo_data$phy,
    type = "fan",
    show.tip.label = FALSE,
    no.margin = TRUE,
    edge.color = edge_col,
    edge.width = 4
  )
  
  coords <- get("last_plot.phylo", envir = ape::.PlotPhyloEnv)
  x_coords <- coords$xx[seq_along(phylo_data$phy$tip.label)]
  y_coords <- coords$yy[seq_along(phylo_data$phy$tip.label)]
  x_range <- diff(range(coords$xx, na.rm = TRUE))
  y_range <- diff(range(coords$yy, na.rm = TRUE))
  img_half_w <- 0.105 * x_range
  img_half_h <- 0.105 * y_range
  
  for (i in seq_along(phylo_data$phy$tip.label)) {
    node_id <- phylo_data$phy$tip.label[[i]]
    raster <- rasters[[node_id]]
    if (is.null(raster)) {
      next
    }
    x <- x_coords[[i]]
    y <- y_coords[[i]]
    graphics::rect(
      x - img_half_w,
      y - img_half_h,
      x + img_half_w,
      y + img_half_h,
      border = "cyan4",
      lwd = 2
    )
    graphics::rasterImage(
      raster,
      x - img_half_w,
      y - img_half_h,
      x + img_half_w,
      y + img_half_h,
      interpolate = TRUE
    )
  }
  if (isTRUE(include_legend)) {
    graphics::legend(
      "bottomleft",
      legend = sprintf("P%s", passage_levels),
      col = passage_palette,
      lwd = 3,
      bty = "n",
      cex = cloneid_figure_text_sizes$panel_a_plot_legend_cex*0.5,
      inset = 0.01,
      title = "Passage"
    )
  }
  invisible(list(phylo = phylo_data$phy, overlay_map = overlay_map, passage_palette = passage_palette))
}

render_panel_a_tree_only_png <- function(subtree_rds, panel_a_zip, cache_dir, output_path) {
  grDevices::png(output_path, width = 3200, height = 2200, res = 220)
  plot_panel_a(
    subtree_rds = subtree_rds,
    panel_a_zip = panel_a_zip,
    cache_dir = cache_dir,
    include_legend = FALSE
  )
  grDevices::dev.off()
  output_path
}

build_panel_a_legend_image <- function(subtree_rds, width = 520, height = 1200) {
  x <- read_subtree_bundle(subtree_rds)
  passaging <- x[["tables"]][["Passaging"]]
  layout <- panel_a_tree_layout(passaging, root_id = panel_a_root_id)
  passage_levels <- sort(unique(layout$edges$child_passage))
  passage_palette <- grDevices::colorRampPalette(
    c("#d8d2c4", "#c58f2f", "#6f8f3a", "#28536b")
  )(length(passage_levels))
  
  legend_path <- tempfile(fileext = ".png")
  grDevices::png(legend_path, width = width, height = height, res = 200, bg = "white")
  op <- graphics::par(mar = c(0, 0, 0, 0))
  graphics::plot.new()
  graphics::legend(
    "center",
    legend = sprintf("P%s", passage_levels),
    col = passage_palette,
    lwd = 8,
    bty = "n",
    cex = cloneid_figure_text_sizes$panel_a_legend_image_cex,
    y.intersp = 1.2,
    title = "Passage"
  )
  graphics::par(op)
  grDevices::dev.off()
  magick::image_read(legend_path)
}

save_panel_a_outputs <- function(subtree_rds,
                                 root_dir = ".",
                                 panel_a_zip = NULL,
                                 output_panels_dir,
                                 cache_dir) {
  panel_a_zip <- find_panel_a_zip(root_dir = root_dir, panel_a_zip = panel_a_zip)
  dir.create(output_panels_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  
  panel_path <- file.path(output_panels_dir, "panel_A_circular_phylogeny.png")
  overlay_csv <- file.path(cache_dir, "panel_a_selected_overlays.csv")
  
  png(panel_path, width = 2200, height = 2200, res = 220)
  plot_result <- plot_panel_a(
    subtree_rds = subtree_rds,
    panel_a_zip = panel_a_zip,
    cache_dir = cache_dir
  )
  dev.off()
  
  utils::write.csv(plot_result$overlay_map, overlay_csv, row.names = FALSE)
  
  list(
    panel_path = panel_path,
    overlay_map = plot_result$overlay_map,
    files = data.frame(
      artifact = c("panel_A", "panel_a_overlay_map"),
      path = c(panel_path, overlay_csv),
      stringsAsFactors = FALSE
    )
  )
}

plot_panel_a0 <- function(subtree_rds,
                          root_id = "SNU-668_rK_A0_seed",
                          highlighted_root_id = panel_a_root_id) {
  x <- read_subtree_bundle(subtree_rds)
  passaging <- x[["tables"]][["Passaging"]]
  layout <- full_subtree_tree_layout(passaging, root_id = root_id)
  highlighted_ids <- descendant_ids(passaging, highlighted_root_id)
  highlighted_edges <- layout$edges$child %in% highlighted_ids
  
  regime_colors <- c(
    "control" = "#8c6d31",
    "K" = "#4f86b8",
    "r" = "#bf5b17",
    "mixed_root" = "#666666"
  )
  edge_col <- regime_colors[layout$edges$regime]
  edge_col[is.na(edge_col)] <- "#999999"
  
  par(mar = c(0.2, 0.2, 0.2, 0.2), xaxs = "i", yaxs = "i")
  plot.new()
  plot.window(xlim = c(-0.02, 1.02), ylim = c(-0.02, 1.02))
  
  segments(layout$edges$x0, layout$edges$y0, layout$edges$x1, layout$edges$y1, col = edge_col, lwd = 0.6)
  if (any(highlighted_edges)) {
    segments(
      layout$edges$x0[highlighted_edges],
      layout$edges$y0[highlighted_edges],
      layout$edges$x1[highlighted_edges],
      layout$edges$y1[highlighted_edges],
      col = "#b2182b",
      lwd = 1.4
    )
  }
  points(layout$nodes$x, layout$nodes$y, pch = 16, cex = 0.2, col = "#444444")
  invisible(layout)
}

render_panel_a0_png <- function(subtree_rds,
                                output_path,
                                root_id = "SNU-668_rK_A0_seed",
                                highlighted_root_id = panel_a_root_id) {
  grDevices::png(output_path, width = 1400, height = 4200, res = 220)
  plot_panel_a0(
    subtree_rds = subtree_rds,
    root_id = root_id,
    highlighted_root_id = highlighted_root_id
  )
  grDevices::dev.off()
  output_path
}

verify_panel_zip_contents <- function(zip_dir, panel_a_manifest, panel_de_manifest) {
  zip_files <- sort(Sys.glob(file.path(zip_dir, "panel*.zip")))
  if (length(zip_files) == 0) {
    return(list(
      summary = data.frame(
        zip_file = character(),
        detected_panel = character(),
        files_in_zip = integer(),
        expected_targets = integer(),
        matched_targets = integer(),
        status = character(),
        stringsAsFactors = FALSE
      ),
      details = list()
    ))
  }
  
  expected_a <- unique(panel_a_manifest$passaging_id)
  expected_de <- unique(panel_de_manifest$passaging_id)
  
  check_one_zip <- function(zip_path) {
    listing <- utils::unzip(zip_path, list = TRUE)
    members <- listing$Name
    
    matched_a <- expected_a[vapply(expected_a, function(id) {
      any(grepl(id, members, fixed = TRUE))
    }, logical(1))]
    matched_de <- expected_de[vapply(expected_de, function(id) {
      any(grepl(id, members, fixed = TRUE))
    }, logical(1))]
    
    count_a <- length(matched_a)
    count_de <- length(matched_de)
    
    if (count_a >= count_de) {
      detected_panel <- "A"
      expected_targets <- length(expected_a)
      matched_targets <- count_a
      missing_targets <- setdiff(expected_a, matched_a)
    } else {
      detected_panel <- "D/E"
      expected_targets <- length(expected_de)
      matched_targets <- count_de
      missing_targets <- setdiff(expected_de, matched_de)
    }
    
    status <- if (matched_targets == expected_targets) {
      "complete"
    } else if (matched_targets > 0) {
      "partial"
    } else {
      "no_expected_content_detected"
    }
    
    list(
      summary = data.frame(
        zip_file = basename(zip_path),
        detected_panel = detected_panel,
        files_in_zip = nrow(listing),
        expected_targets = expected_targets,
        matched_targets = matched_targets,
        status = status,
        stringsAsFactors = FALSE
      ),
      details = list(
        zip_file = zip_path,
        detected_panel = detected_panel,
        matched_targets = if (detected_panel == "A") matched_a else matched_de,
        missing_targets = missing_targets,
        members = members
      )
    )
  }
  
  checked <- lapply(zip_files, check_one_zip)
  
  list(
    summary = do.call(rbind, lapply(checked, `[[`, "summary")),
    details = lapply(checked, `[[`, "details")
  )
}

find_panel_de_zip <- function(root_dir = ".", panel_de_zip = NULL) {
  zip_path <- if (is.null(panel_de_zip)) file.path(root_dir, "input", "subtree", "panelDE.zip") else panel_de_zip
  if (!file.exists(zip_path)) {
    stop(sprintf("Panel D/E zip not found at '%s'.", zip_path))
  }
  normalizePath(zip_path, winslash = "/", mustWork = TRUE)
}

extract_panel_de_zip <- function(panel_de_zip, cache_dir) {
  root <- file.path(cache_dir, "panelDE_unpacked")
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  utils::unzip(panel_de_zip, exdir = root)
  root
}

read_panel_de_genome_profiles <- function(panel_de_zip, cache_dir) {
  root <- extract_panel_de_zip(panel_de_zip, cache_dir)
  genome_files <- list.files(
    file.path(root, "perspectives"),
    pattern = "^GenomePerspective\\.rds$",
    recursive = TRUE,
    full.names = TRUE
  )
  if (length(genome_files) == 0) {
    stop(sprintf("No GenomePerspective.rds files found in '%s'.", panel_de_zip))
  }
  
  profile_list <- lapply(genome_files, function(path) {
    x <- readRDS(path)
    profile <- x$profile
    interval_id <- rownames(profile)
    keep_rows <- grepl("^[0-9]+:[0-9]+-[0-9]+$", interval_id)
    profile <- profile[keep_rows, , drop = FALSE]
    interval_id <- interval_id[keep_rows]
    rownames(profile) <- interval_id
    
    list(
      passaging_id = x$passaging_id,
      perspective_type = x$perspective_type,
      profile = profile,
      file = path
    )
  })
  
  names(profile_list) <- vapply(profile_list, `[[`, character(1), "passaging_id")
  profile_list
}

panel_de_cell_metrics <- function(profile_list) {
  rows <- lapply(names(profile_list), function(id) {
    profile <- profile_list[[id]]$profile
    data.frame(
      passaging_id = id,
      regime = selection_regime_from_id(id),
      replicate = replicate_from_id(id),
      cell_id = colnames(profile),
      mean_autosomal_copy_number = colMeans(profile, na.rm = TRUE),
      total_autosomal_copy_number = colSums(profile, na.rm = TRUE),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

plot_panel_d <- function(cell_metrics) {
  cell_metrics$passaging_id <- factor(
    cell_metrics$passaging_id,
    levels = panel_de_selected_nodes
  )
  lineage_colors <- cloneid_lineage_palette[panel_de_selected_nodes]
  
  ggplot2::ggplot(
    cell_metrics,
    ggplot2::aes(
      x = passaging_id,
      y = mean_autosomal_copy_number,
      fill = passaging_id,
      color = passaging_id
    )
  ) +
    ggplot2::geom_violin(alpha = 0.25, width = 0.9, trim = FALSE) +
    ggplot2::geom_jitter(width = 0.15, height = 0, size = 1.3, alpha = 0.8) +
    ggplot2::scale_fill_manual(values = lineage_colors, drop = FALSE) +
    ggplot2::scale_color_manual(values = lineage_colors, drop = FALSE) +
    ggplot2::labs(
      x = NULL,
      y = "Mean autosomal copy number per cell"
    ) +
    cloneid_figure_theme() +
    ggplot2::theme(
      axis.text.x = ggplot2::element_blank(),
      axis.ticks.x = ggplot2::element_blank(),
      legend.position = "right",
      axis.title.y = ggplot2::element_text(size = 10) # <-- Add this line
    )
}

parse_panel_e_intervals <- function(interval_ids) {
  chrom <- sub(":.*$", "", interval_ids)
  coords <- sub("^[0-9]+:", "", interval_ids)
  start <- suppressWarnings(as.numeric(sub("-.*$", "", coords)))
  end <- suppressWarnings(as.numeric(sub("^.*-", "", coords)))
  
  data.frame(
    interval_id = interval_ids,
    chromosome = chrom,
    chromosome_num = suppressWarnings(as.integer(chrom)),
    start = start,
    end = end,
    stringsAsFactors = FALSE
  )
}

build_panel_e_matrix <- function(profile_list) {
  common_rows <- Reduce(intersect, lapply(profile_list, function(x) rownames(x$profile)))
  interval_meta <- parse_panel_e_intervals(common_rows)
  interval_meta <- interval_meta[order(interval_meta$chromosome_num, interval_meta$start, interval_meta$end), , drop = FALSE]
  common_rows <- interval_meta$interval_id
  ordered_ids <- panel_de_selected_nodes[panel_de_selected_nodes %in% names(profile_list)]
  mats <- lapply(ordered_ids, function(id) {
    m <- profile_list[[id]]$profile[common_rows, , drop = FALSE]
    colnames(m) <- paste(id, colnames(m), sep = "::")
    m
  })
  combined <- do.call(cbind, mats)
  list(matrix = combined, interval_meta = interval_meta, lineage_ids = ordered_ids)
}

panel_e_annotation_colors <- function(lineage_ids) {
  cols <- unname(cloneid_lineage_palette[lineage_ids])
  names(cols) <- lineage_ids
  cols
}

plot_panel_e <- function(profile_list, silent = TRUE) {
  built <- build_panel_e_matrix(profile_list)
  mat <- built$matrix
  interval_meta <- built$interval_meta
  
  transposed_matrix <- t(mat)
  sample_id <- sub("::.*$", "", rownames(transposed_matrix))
  annotation_row <- data.frame(lineage = sample_id, stringsAsFactors = FALSE)
  rownames(annotation_row) <- rownames(transposed_matrix)
  
  lineage_ids <- built$lineage_ids
  row_hclust <- stats::hclust(stats::dist(transposed_matrix), method = "ward.D")
  cluster_ids <- stats::cutree(row_hclust, k = 5)
  annotation_row$cluster <- factor(
    sprintf("C%s", cluster_ids),
    levels = sprintf("C%s", sort(unique(cluster_ids)))
  )
  cluster_colors <- c(
    "C1" = "#6a3d9a",
    "C2" = "#1b9e77",
    "C3" = "#e7298a",
    "C4" = "#66a61e",
    "C5" = "#7570b3"
  )
  annotation_colors <- list(
    lineage = panel_e_annotation_colors(lineage_ids),
    cluster = cluster_colors
  )
  
  my_breaks <- seq(0, 9, length.out = 202)
  my_colors <- grDevices::colorRampPalette(
    wesanderson::wes_palette("Zissou1", 42, type = "continuous")
  )(200)
  
  colnames(transposed_matrix) <- interval_meta$chromosome[match(colnames(transposed_matrix), interval_meta$interval_id)]
  
  pheatmap::pheatmap(
    transposed_matrix,
    color = my_colors,
    breaks = my_breaks,
    annotation_row = annotation_row,
    annotation_colors = annotation_colors,
    cluster_rows = row_hclust,
    cluster_cols = FALSE,
    show_rownames = FALSE,
    show_colnames = TRUE,
    border_color = NA,
    main = "",
    fontsize = cloneid_figure_text_sizes$panel_e_fontsize,
    fontsize_col = cloneid_figure_text_sizes$panel_e_fontsize_col,
    fontsize_row = cloneid_figure_text_sizes$panel_e_fontsize_row,
    silent = silent
  )
}

save_de_outputs <- function(root_dir = ".", panel_de_zip = NULL, output_panels_dir, cache_dir) {
  panel_de_zip <- find_panel_de_zip(root_dir = root_dir, panel_de_zip = panel_de_zip)
  profile_list <- read_panel_de_genome_profiles(panel_de_zip, cache_dir = cache_dir)
  cell_metrics <- panel_de_cell_metrics(profile_list)
  panel_d_plot <- plot_panel_d(cell_metrics)
  panel_e_plot <- plot_panel_e(profile_list, silent = TRUE)
  
  dir.create(output_panels_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  
  panel_d_file <- file.path(output_panels_dir, "panel_D_ploidy_surrogate.png")
  panel_e_file <- file.path(output_panels_dir, "panel_E_genome_heatmap.png")
  metrics_csv <- file.path(cache_dir, "panel_de_cell_metrics.csv")
  
  ggplot2::ggsave(panel_d_file, plot = panel_d_plot, width = 8, height = 3, dpi = 300)
  grDevices::png(panel_e_file, width = 8, height = 7.5, units = "in", res = 300)
  grid::grid.newpage()
  grid::grid.draw(panel_e_plot$gtable)
  grDevices::dev.off()
  utils::write.csv(cell_metrics, metrics_csv, row.names = FALSE)
  
  list(
    profile_list = profile_list,
    cell_metrics = cell_metrics,
    panel_d_plot = panel_d_plot,
    panel_e_plot = panel_e_plot,
    files = data.frame(
      artifact = c("panel_D_png", "panel_E_png", "panel_DE_metrics_csv"),
      path = c(panel_d_file, panel_e_file, metrics_csv),
      stringsAsFactors = FALSE
    )
  )
}
