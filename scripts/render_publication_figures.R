#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(ggplot2)
  library(patchwork)
  library(svglite)
  library(ragg)
  library(ggrepel)
  library(scales)
  library(dplyr)
  library(tidyr)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: render_publication_figures.R <input_json> <output_dir>")
}

input_path <- args[[1]]
output_dir <- args[[2]]
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

payload <- fromJSON(input_path, simplifyVector = FALSE)
series <- payload$series

stage_palette <- c(
  "Diagnosed" = "#5b74d6",
  "On ART" = "#16886f",
  "Suppressed" = "#db6b2c"
)

positive_color <- "#16886f"
negative_color <- "#c85f1f"
grid_color <- "#d6ddd9"
bg_color <- "#fffaf0"
panel_color <- "#fffdf8"

quarter_to_decimal <- function(period) {
  matches <- regmatches(period, regexec("^(\\d{4}) Q([1-4])$", period))[[1]]
  if (length(matches) == 3) {
    return(as.numeric(matches[2]) + (as.numeric(matches[3]) - 1) / 4)
  }
  suppressWarnings(as.numeric(period))
}

save_plot_assets <- function(plot, basename, width = 13, height = 8) {
  svg_path <- file.path(output_dir, paste0(basename, ".svg"))
  pdf_path <- file.path(output_dir, paste0(basename, ".pdf"))
  png_path <- file.path(output_dir, paste0(basename, ".png"))

  svglite::svglite(svg_path, width = width, height = height, bg = "white")
  print(plot)
  dev.off()

  grDevices::pdf(pdf_path, width = width, height = height, useDingbats = FALSE, bg = "white")
  print(plot)
  dev.off()

  ragg::agg_png(png_path, width = width * 180, height = height * 180, units = "px", res = 180, background = "white")
  print(plot)
  dev.off()
}

epi_theme <- function(base_size = 13) {
  theme_minimal(base_size = base_size, base_family = "sans") +
    theme(
      plot.background = element_rect(fill = bg_color, colour = NA),
      panel.background = element_rect(fill = panel_color, colour = "#ebe4d8", linewidth = 0.6),
      panel.grid.major = element_line(colour = grid_color, linewidth = 0.5),
      panel.grid.minor = element_blank(),
      axis.title = element_text(face = "bold", colour = "#314540"),
      axis.text = element_text(colour = "#516661"),
      plot.title = element_text(face = "bold", family = "serif", size = base_size + 6, colour = "#14231f"),
      plot.subtitle = element_text(size = base_size + 1, colour = "#566a64", margin = margin(b = 10)),
      plot.caption = element_text(size = base_size - 2, colour = "#667973"),
      strip.text = element_text(face = "bold", family = "serif", size = base_size + 1, colour = "#14231f"),
      legend.position = "top",
      legend.title = element_blank(),
      legend.text = element_text(size = base_size),
      plot.margin = margin(10, 12, 8, 12)
    )
}

compact_theme <- function(base_size = 12) {
  epi_theme(base_size) +
    theme(
      plot.title = element_text(size = base_size + 3),
      plot.subtitle = element_text(size = base_size),
      axis.text.x = element_text(size = base_size - 1),
      axis.text.y = element_text(size = base_size - 1)
    )
}

build_national_cascade <- function(data) {
  stage_map <- c(
    "first_95" = "Diagnosed",
    "second_95" = "On ART",
    "third_95" = "Suppressed"
  )

  quarterly <- bind_rows(lapply(data$rows, function(row) {
    if (length(row$points) == 0) return(NULL)
    tibble(
      stage = stage_map[[row$series_id]],
      period = vapply(row$points, `[[`, "", "period"),
      x = vapply(row$points, function(point) quarter_to_decimal(point$period), numeric(1)),
      value = as.numeric(vapply(row$points, `[[`, 0, "value")),
      latest_period = row$latest_period,
      latest_value = as.numeric(row$latest_value),
      gap = as.numeric(row$gap_to_target)
    )
  }))
  quarterly$stage <- factor(quarterly$stage, levels = c("Diagnosed", "On ART", "Suppressed"))

  annual <- bind_rows(lapply(data$rows, function(row) {
    if (length(row$official_annual) == 0) return(NULL)
    tibble(
      stage = stage_map[[row$series_id]],
      x = as.numeric(vapply(row$official_annual, `[[`, 0, "year")),
      value = as.numeric(vapply(row$official_annual, `[[`, 0, "value"))
    )
  }))
  annual$stage <- factor(annual$stage, levels = c("Diagnosed", "On ART", "Suppressed"))

  metrics <- tibble(
    stage = factor(stage_map[vapply(data$rows, `[[`, "", "series_id")], levels = c("Diagnosed", "On ART", "Suppressed")),
    latest_value = as.numeric(vapply(data$rows, `[[`, 0, "latest_value")),
    latest_period = vapply(data$rows, `[[`, "", "latest_period"),
    gap = as.numeric(vapply(data$rows, `[[`, 0, "gap_to_target"))
  )

  latest_counts <- setNames(
    lapply(data$rows, function(row) {
      points <- row$count_points
      if (length(points) == 0) return(NA_real_)
      as.numeric(points[[length(points)]][["value"]])
    }),
    vapply(data$rows, `[[`, "", "series_id")
  )

  estimated_plhiv <- latest_counts[["first_95"]] / (metrics$latest_value[metrics$stage == "Diagnosed"] / 100)
  diagnosed <- latest_counts[["first_95"]]
  on_art <- latest_counts[["second_95"]]
  suppressed <- latest_counts[["third_95"]]

  bullet_df <- metrics %>%
    mutate(
      anchor = stage,
      target = 95,
      card_value = sprintf("%.0f%%", latest_value),
      period_label = latest_period,
      gap_label = sprintf("Gap %.0f points", gap),
      card_xmin = 0,
      card_xmax = 100
    )

  bullet <- ggplot(bullet_df, aes(y = anchor)) +
    geom_segment(aes(x = 0, xend = 100, yend = anchor), linewidth = 9.5, colour = "#ebefea", lineend = "round") +
    geom_segment(aes(x = 0, xend = 95, yend = anchor), linewidth = 9.5, colour = "#f4e6d7", lineend = "round") +
    geom_segment(aes(x = 0, xend = latest_value, yend = anchor, colour = stage), linewidth = 9.5, lineend = "round") +
    geom_point(aes(x = latest_value, fill = stage), shape = 21, size = 6.2, stroke = 1.25, colour = "white") +
    geom_vline(xintercept = 95, linetype = "22", linewidth = 0.7, colour = "#b45521") +
    geom_text(aes(x = 2, y = anchor, label = card_value, colour = stage), hjust = 0, vjust = 1.95, size = 7.2, fontface = "bold", family = "serif") +
    geom_text(aes(x = 2, y = anchor, label = period_label), hjust = 0, vjust = 0.5, size = 3.6, colour = "#4d615b") +
    geom_text(aes(x = 2, y = anchor, label = gap_label), hjust = 0, vjust = -0.75, size = 3.5, colour = "#6b7e78") +
    annotate("text", x = 95, y = 3.45, label = "95 target", hjust = 1.05, size = 3.6, colour = "#9b3e25", fontface = "bold") +
    facet_wrap(~stage, nrow = 1) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_x_continuous(
      limits = c(0, 100),
      breaks = c(0, 50, 95, 100),
      labels = c("0%", "50%", "95%", "100%")
    ) +
    scale_y_discrete(NULL) +
    labs(x = NULL, y = NULL) +
    compact_theme(12) +
    theme(
      legend.position = "none",
      strip.background = element_blank(),
      strip.text = element_text(size = 16, face = "bold", family = "serif"),
      panel.grid = element_blank(),
      axis.text.y = element_blank(),
      panel.grid.minor = element_blank(),
      axis.text.x = element_text(size = 10, colour = "#6b7e78"),
      axis.ticks.y = element_blank()
    )

  latest_quarterly <- quarterly %>%
    group_by(stage) %>%
    arrange(desc(x)) %>%
    slice_head(n = 1) %>%
    ungroup()

  latest_quarterly$label <- c("Diagnosed", "On ART", "Suppressed")

  timeline <- ggplot() +
    geom_hline(yintercept = 95, linetype = "22", linewidth = 0.6, colour = "#b45521") +
    geom_line(data = annual, aes(x = x, y = value, colour = stage, group = stage), linewidth = 0.9, alpha = 0.18, linetype = "22", na.rm = TRUE) +
    geom_point(data = annual, aes(x = x, y = value), shape = 21, size = 2.5, stroke = 1.0, fill = panel_color, colour = "#9fb5d0", alpha = 0.95, na.rm = TRUE) +
    geom_line(data = quarterly, aes(x = x, y = value, colour = stage, group = stage), linewidth = 2.2, na.rm = TRUE) +
    geom_point(data = quarterly, aes(x = x, y = value, fill = stage), shape = 21, size = 3.2, stroke = 0.9, colour = "white", na.rm = TRUE) +
    ggrepel::geom_text_repel(
      data = latest_quarterly,
      aes(x = x, y = value, label = sprintf("%s  %.0f%%", label, value), colour = stage),
      direction = "y",
      hjust = 0,
      nudge_x = 0.32,
      size = 4.0,
      fontface = "bold",
      min.segment.length = 0,
      box.padding = 0.28,
      max.overlaps = 20,
      show.legend = FALSE
    ) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_x_continuous(
      limits = c(2015, 2026.4),
      breaks = c(2015, 2018, 2020, 2023.25, 2025.75),
      labels = c("2015", "2018", "2020", "2023 Q2", "2025 Q4")
    ) +
    scale_y_continuous(limits = c(25, 100), labels = label_percent(scale = 1), breaks = c(30, 40, 50, 60, 70, 80, 90, 95, 100)) +
    labs(
      title = "Observed trajectory",
      x = NULL,
      y = "Coverage"
    ) +
    epi_theme(12) +
    theme(legend.position = "none") +
    guides(fill = "none", colour = "none")

  waterfall <- tibble(
    step = factor(
      c("Estimated PLHIV", "Undiagnosed", "Not on ART", "Not suppressed", "Suppressed"),
      levels = c("Estimated PLHIV", "Undiagnosed", "Not on ART", "Not suppressed", "Suppressed")
    ),
    xmin = c(0.65, 1.65, 2.65, 3.65, 4.65),
    xmax = c(1.35, 2.35, 3.35, 4.35, 5.35),
    start = c(0, diagnosed, on_art, suppressed, 0),
    end = c(estimated_plhiv, estimated_plhiv, diagnosed, on_art, suppressed),
      fill_key = c("Estimated PLHIV", "Undiagnosed", "Off ART", "Not suppressed", "Suppressed"),
      label = c(
        label_number(scale_cut = cut_short_scale())(estimated_plhiv),
        paste0("-", label_number(scale_cut = cut_short_scale())(estimated_plhiv - diagnosed)),
        paste0("-", label_number(scale_cut = cut_short_scale())(diagnosed - on_art)),
        paste0("-", label_number(scale_cut = cut_short_scale())(on_art - suppressed)),
      paste0(label_number(scale_cut = cut_short_scale())(suppressed), "\n", round(suppressed / estimated_plhiv * 100), "% retained")
    )
  ) %>%
    mutate(ymin = pmin(start, end), ymax = pmax(start, end))

  connectors <- tibble(
    x = c(1.35, 2.35, 3.35, 4.35),
    xend = c(1.65, 2.65, 3.65, 4.65),
    y = c(estimated_plhiv, diagnosed, on_art, suppressed),
    yend = c(estimated_plhiv, diagnosed, on_art, suppressed)
  )

  waterfall_plot <- ggplot(waterfall) +
      geom_rect(aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = fill_key), colour = "white", linewidth = 0.8) +
      geom_segment(data = connectors, aes(x = x, xend = xend, y = y, yend = yend), linewidth = 0.75, colour = "#7a8f88") +
      geom_text(aes(x = (xmin + xmax) / 2, y = ymax + estimated_plhiv * 0.025, label = label), size = 3.8, fontface = "bold", colour = "#304641") +
      scale_fill_manual(values = c("Estimated PLHIV" = "#d9e7e2", "Undiagnosed" = "#e88f4f", "Off ART" = "#d86a2b", "Not suppressed" = "#c4561b", "Suppressed" = "#0f7c66")) +
      scale_x_continuous(breaks = 1:5, labels = levels(waterfall$step), expand = expansion(mult = c(0.04, 0.04))) +
      scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()), expand = expansion(mult = c(0, 0.14))) +
      labs(
        title = "Latest cascade losses in people",
        x = NULL,
        y = NULL
      ) +
    compact_theme(11.5) +
    theme(
      legend.position = "none",
      axis.text.x = element_text(face = "bold")
    )

    top_row <- bullet
    bottom_row <- timeline | waterfall_plot + plot_layout(widths = c(2.1, 1.1))
    (top_row / bottom_row) + plot_layout(heights = c(0.82, 1.28)) +
      plot_annotation(
        title = "National 95-95-95 board, Philippines",
        subtitle = "Three views of the same endpoint: current target position, observed trajectory, and the latest cascade losses in people."
      )
}

build_regional_matrix <- function(data) {
  rows <- bind_rows(lapply(data$rows, as_tibble))
  if (!nrow(rows)) return(NULL)

  rows <- rows %>%
    mutate(region = factor(region, levels = rev(region)))
  matrix_df <- rows %>%
    select(region, diagnosis, treatment, suppression) %>%
    pivot_longer(-region, names_to = "stage", values_to = "value") %>%
    mutate(stage = recode(stage, diagnosis = "Diagnosed", treatment = "On ART", suppression = "Suppressed"))

  gap_df <- rows %>%
    transmute(region, mean_gap = as.numeric(mean_gap)) %>%
    mutate(region = factor(region, levels = levels(rows$region)))

  heatmap <- ggplot(matrix_df, aes(x = stage, y = region, fill = value)) +
    geom_tile(colour = "white", linewidth = 1.2, width = 0.96, height = 0.94) +
    geom_text(aes(label = sprintf("%.0f%%", value)), size = 4.3, colour = "#17302a", fontface = "bold") +
    scale_fill_gradientn(
      colours = c("#f6d7bf", "#e6c77b", "#86c3b1", "#0f7c66"),
      limits = c(35, 95),
      labels = label_percent(scale = 1)
    ) +
    labs(
      title = paste0("Regional stage matrix  ", data$period_label),
      x = NULL,
      y = NULL
    ) +
    epi_theme(12) +
    theme(
      legend.position = "bottom",
      axis.text.x = element_text(face = "bold", size = 12),
      panel.grid = element_blank(),
      axis.text.y = element_text(size = 11),
      legend.key.width = grid::unit(1.8, "cm")
    )

  gaps <- ggplot(gap_df, aes(x = mean_gap, y = region)) +
    geom_segment(aes(x = 0, xend = mean_gap, yend = region), linewidth = 1.35, colour = "#d3dcd7") +
    geom_point(size = 4.0, colour = "#b45521") +
    geom_text(aes(label = sprintf("%.1f", mean_gap)), hjust = -0.16, size = 4.0, colour = "#304641") +
    scale_x_continuous(expand = expansion(mult = c(0, 0.24))) +
    labs(
      title = "Gap to 95",
      x = "Percentage points below target",
      y = NULL
    ) +
    compact_theme(12) +
    theme(
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank(),
      legend.position = "none",
      panel.grid.major.y = element_blank()
    )

  closest_region <- as.character(rows$region[length(levels(rows$region))])
  widest_gap_region <- as.character(rows$region[1])

  (heatmap | gaps) + plot_layout(widths = c(2.45, 1.05)) +
      plot_annotation(
        title = "Regional stage matrix, latest yearly snapshot",
        subtitle = paste0(closest_region, " is currently closest to the combined 95-95-95 target. ", widest_gap_region, " has the widest average gap.")
      )
}

build_regional_fingerprint_board <- function(data) {
  years <- sort(as.integer(unlist(data$years)))
  if (!length(years)) return(NULL)
  latest_year <- max(years, na.rm = TRUE)
  latest_rows <- bind_rows(lapply(data$rows_by_year[[as.character(latest_year)]], as_tibble))
  if (!nrow(latest_rows)) return(NULL)

  latest_rows <- latest_rows %>%
    mutate(mean_gap = as.numeric(mean_gap))

  latest_burden <- function(region) {
    burden_rows <- bind_rows(lapply(data$region_histories[[region]]$burden, as_tibble))
    if (!nrow(burden_rows)) return(0)
    burden_rows <- burden_rows %>%
      mutate(
        year = as.integer(year),
        burden = as.numeric(ltfu) + as.numeric(not_on_treatment)
      ) %>%
      arrange(year)
    tail(burden_rows$burden, 1)
  }

  best_region <- latest_rows %>% arrange(mean_gap) %>% slice(1) %>% pull(region)
  worst_region <- latest_rows %>% arrange(desc(mean_gap)) %>% slice(1) %>% pull(region)
  burden_region <- latest_rows %>%
    mutate(latest_burden = vapply(region, latest_burden, numeric(1))) %>%
    arrange(desc(latest_burden)) %>%
    slice(1) %>%
    pull(region)

  chosen_regions <- unique(c(best_region, burden_region, worst_region))
  if (!length(chosen_regions)) return(NULL)

  cascade_rows <- bind_rows(lapply(chosen_regions, function(region) {
    history <- bind_rows(lapply(data$region_histories[[region]]$cascade, as_tibble))
    if (!nrow(history)) return(NULL)
    history <- history %>%
      mutate(year = as.integer(year)) %>%
      arrange(year)
    latest <- history %>% slice_tail(n = 1)
    previous <- if (nrow(history) > 1) history %>% slice_tail(n = 2) %>% slice_head(n = 1) else NULL
    tibble(
      region = region,
      year = latest$year,
      stage = c("Diagnosed", "On ART", "Suppressed"),
      value = c(as.numeric(latest$diagnosis), as.numeric(latest$treatment), as.numeric(latest$suppression)),
      delta = c(
        if (!is.null(previous)) as.numeric(latest$diagnosis) - as.numeric(previous$diagnosis) else NA_real_,
        if (!is.null(previous)) as.numeric(latest$treatment) - as.numeric(previous$treatment) else NA_real_,
        if (!is.null(previous)) as.numeric(latest$suppression) - as.numeric(previous$suppression) else NA_real_
      ),
      gap = c(
        95 - as.numeric(latest$diagnosis),
        95 - as.numeric(latest$treatment),
        95 - as.numeric(latest$suppression)
      )
    )
  }))
  if (!nrow(cascade_rows)) return(NULL)

  role_labels <- c(
    setNames("Closest to target", best_region),
    setNames("Largest leakage burden", burden_region),
    setNames("Widest gap", worst_region)
  )
  cascade_rows <- cascade_rows %>%
    mutate(
      stage = factor(stage, levels = c("Diagnosed", "On ART", "Suppressed")),
      role = unname(role_labels[region]),
      region_panel = paste0(region, "\n", role)
    )

  summary_rows <- cascade_rows %>%
    group_by(region, role, region_panel) %>%
    summarise(
      mean_gap = mean(gap, na.rm = TRUE),
      max_value = max(value, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      burden = vapply(region, latest_burden, numeric(1)),
      summary_label = sprintf("Mean gap %.1f pts\nLeakage burden %s", mean_gap, label_number(scale_cut = cut_short_scale())(burden))
    )

  ggplot(cascade_rows, aes(x = stage, y = value, colour = stage, group = 1)) +
    geom_hline(yintercept = 95, linewidth = 0.7, linetype = "22", colour = "#b45521") +
    geom_col(aes(fill = stage), alpha = 0.18, width = 0.56, colour = NA) +
    geom_line(linewidth = 1.2, colour = "#7b8e88", show.legend = FALSE) +
    geom_point(size = 4.3, stroke = 1.0, fill = "white", shape = 21) +
    geom_text(
      aes(label = sprintf("%.0f%%", value)),
      nudge_y = 4.0,
      size = 4.0,
      fontface = "bold",
      colour = "#17302a",
      show.legend = FALSE
    ) +
    geom_text(
      data = cascade_rows %>% filter(!is.na(delta)),
      aes(label = sprintf("%+.0f", delta)),
      nudge_y = -6.8,
      size = 3.4,
      colour = "#6a7a75",
      show.legend = FALSE
    ) +
    geom_text(
      data = summary_rows,
      aes(x = 2.85, y = 26.5, label = summary_label),
      hjust = 1,
      vjust = 0,
      inherit.aes = FALSE,
      size = 3.6,
      colour = "#4c5f59"
    ) +
    facet_wrap(~region_panel, nrow = 1) +
    scale_colour_manual(values = stage_palette) +
    scale_fill_manual(values = stage_palette) +
    scale_y_continuous(limits = c(24, 100), breaks = c(30, 40, 50, 60, 70, 80, 90, 95, 100), labels = label_percent(scale = 1)) +
    labs(
      title = paste0("Regional fingerprint board  ", latest_year),
      subtitle = "Three exemplar regions are shown using observed yearly diagnosis, treatment, and suppression coverage. Labels above points show current values; labels below show year-over-year change.",
      x = NULL,
      y = "Coverage"
    ) +
    epi_theme(12) +
    theme(
      legend.position = "top",
      strip.text = element_text(size = 15, lineheight = 1.05),
      axis.text.x = element_text(face = "bold", size = 12),
      panel.grid.major.x = element_blank()
    )
}

build_anomaly_board <- function(data) {
  perf <- bind_rows(lapply(data$performance_burden_rows, as_tibble))
  leak <- bind_rows(lapply(data$leakage_rows, as_tibble))
  if (!nrow(perf) || !nrow(leak)) return(NULL)

  perf <- perf %>%
    mutate(
      leakage_total = leakage_burden,
      sign = ifelse(residual >= 0, "Above expected", "Below expected"),
      stage = factor(stage, levels = c("Treatment after diagnosis", "Suppression after treatment"))
    )

  label_df <- perf %>%
    arrange(desc(abs(residual) + leakage_total / max(leakage_total, na.rm = TRUE) * 4)) %>%
    slice_head(n = min(8, nrow(perf)))

  quad <- ggplot(perf, aes(x = residual, y = leakage_burden)) +
    geom_vline(xintercept = 0, linewidth = 0.7, colour = "#7a8f88") +
    geom_point(aes(size = leakage_total, fill = stage, colour = sign), shape = 21, stroke = 1.0, alpha = 0.94) +
    ggrepel::geom_text_repel(
      data = label_df,
      aes(label = region),
      size = 4.1,
      min.segment.length = 0,
      box.padding = 0.3,
      max.overlaps = 50,
      colour = "#314540"
    ) +
    scale_fill_manual(values = c("Treatment after diagnosis" = "#0f7c66", "Suppression after treatment" = "#db6b2c")) +
    scale_colour_manual(values = c("Above expected" = "#1a8b73", "Below expected" = "#c85f1f")) +
    scale_size_continuous(range = c(5, 18), labels = label_number(scale_cut = cut_short_scale())) +
      scale_x_continuous(labels = function(x) sprintf("%+.0f", x)) +
      scale_y_continuous(trans = "sqrt", labels = label_number(scale_cut = cut_short_scale())) +
      labs(
        title = "Performance versus burden quadrant",
        x = "Residual from expected cascade performance (percentage points)",
        y = paste0("Loss from care burden (", data$period_label, ", square-root scale)"),
        size = "Loss from care",
        fill = "Cascade break",
        colour = "Direction"
      ) +
      epi_theme(12) +
      theme(legend.position = "bottom")

  leak_long <- leak %>%
    transmute(
      region,
      `Lost to follow-up` = ltfu,
      `Not on treatment` = not_on_treatment
    ) %>%
    pivot_longer(-region, names_to = "component", values_to = "value") %>%
    mutate(region = factor(region, levels = rev(leak$region)))

  leak_plot <- ggplot(leak_long, aes(x = value, y = region, fill = component)) +
    geom_col(width = 0.7, position = "stack") +
      scale_fill_manual(values = c("Lost to follow-up" = "#db6b2c", "Not on treatment" = "#c89a25")) +
      scale_x_continuous(labels = label_number(scale_cut = cut_short_scale())) +
      labs(
        title = "Largest leakage burdens",
        x = NULL,
        y = NULL
      ) +
      compact_theme(12)

  (quad | leak_plot) + plot_layout(widths = c(1.7, 1.15)) +
    plot_annotation(
      title = "Performance versus treatment burden",
      subtitle = "The quadrant identifies regions that underperform the fitted cascade pattern, while the ranked bars show where loss from care is concentrated."
    )
}

build_historical_board <- function(data) {
  panel_specs <- list(
    list(key = "cases", title = "Cumulative reported HIV cases", color = "#0f7c66", unit = "count"),
    list(key = "plhiv", title = "People living with HIV", color = "#3565af", unit = "count"),
    list(key = "new_infections", title = "New HIV infections", color = "#b35323", unit = "count"),
    list(key = "aids_deaths", title = "AIDS-related deaths", color = "#8a3f2a", unit = "count")
  )

  plots <- lapply(panel_specs, function(spec) {
    points <- bind_rows(lapply(data[[spec$key]], as_tibble))
    if (!nrow(points)) return(NULL)
    years <- tibble(year = 2015:2025)
    df <- years %>% left_join(points %>% transmute(year = as.integer(year), value = as.numeric(value)), by = "year")
      ggplot(df, aes(x = year, y = value)) +
        geom_area(fill = alpha(spec$color, 0.12), colour = NA, na.rm = TRUE) +
        geom_line(color = spec$color, linewidth = 1.7, na.rm = TRUE) +
        geom_point(data = df %>% filter(!is.na(value)), color = "#db6b2c", fill = "#db6b2c", size = 3.0) +
        scale_x_continuous(breaks = c(2015, 2020, 2025)) +
        scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
        labs(title = spec$title, x = NULL, y = NULL) +
        compact_theme(12)
  })

  wrap_plots(plots, ncol = 2) +
    plot_annotation(
      title = "Long-run burden indicators, Philippines",
      subtitle = "Observed annual values only. Missing years remain blank; no interpolation is applied."
    )
}

build_key_population_board <- function(data) {
  panel_specs <- list(
    list(key = "pregnant_cumulative", title = "Pregnant women diagnosed", color = "#0f7c66", unit = "count"),
    list(key = "tgw_cumulative", title = "TGW diagnosed", color = "#b35323", unit = "count"),
    list(key = "ofw_cumulative", title = "OFW cumulative burden", color = "#16886f", unit = "count"),
    list(key = "youth_share", title = "Youth share of reported cases", color = "#3565af", unit = "percent")
  )

  plots <- lapply(panel_specs, function(spec) {
    points <- bind_rows(lapply(data[[spec$key]], as_tibble))
    if (!nrow(points)) return(NULL)
    years <- tibble(year = 2015:2025)
    df <- years %>% left_join(points %>% transmute(year = as.integer(year), value = as.numeric(value)), by = "year")
      p <- ggplot(df, aes(x = year, y = value)) +
        geom_area(fill = alpha(spec$color, 0.12), colour = NA, na.rm = TRUE) +
        geom_line(color = spec$color, linewidth = 1.8, na.rm = TRUE) +
        geom_point(data = df %>% filter(!is.na(value)), color = "#db6b2c", size = 3.0) +
        scale_x_continuous(breaks = c(2015, 2020, 2025)) +
        labs(title = spec$title, x = NULL, y = NULL) +
        compact_theme(12)
    if (spec$unit == "percent") {
      p <- p + scale_y_continuous(labels = label_percent(scale = 1))
    } else {
      p <- p + scale_y_continuous(labels = label_number(scale_cut = cut_short_scale()))
    }
    p
  })

  wrap_plots(plots, ncol = 2) +
    plot_annotation(
      title = "Key population sentinel panels, Philippines",
      subtitle = "Observed annual values on a shared 2015-2025 window. Missing years remain visible as gaps."
    )
}

render_safe <- function(builder, basename, width, height) {
  tryCatch({
    plot <- builder()
    if (!is.null(plot)) {
      save_plot_assets(plot, basename, width, height)
    }
  }, error = function(e) {
    message("Failed to render ", basename, ": ", conditionMessage(e))
  })
}

render_safe(function() build_national_cascade(series$national_cascade), "national_cascade_board", 15.5, 8.6)
render_safe(function() build_regional_matrix(series$regional_ladder), "regional_stage_matrix", 15.5, 8.8)
render_safe(function() build_regional_fingerprint_board(series$regional_yearly), "regional_fingerprint_board", 15.5, 7.6)
render_safe(function() build_anomaly_board(series$anomalies), "anomaly_board", 15.5, 8.3)
render_safe(function() build_historical_board(series$historical), "historical_board", 15.5, 9.0)
render_safe(function() build_key_population_board(series$key_populations), "key_populations_board", 15.5, 9.2)
