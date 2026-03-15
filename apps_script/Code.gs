const DATA_FOLDER_ID = "YOUR_DRIVE_FOLDER_ID_HERE";
const DEFAULT_LIMIT = 80;
const SMALL_FILE_CACHE_SECONDS = 300;
const SMALL_FILE_CACHE_MAX_CHARS = 90000;

function doGet() {
  return HtmlService.createTemplateFromFile("Index")
    .evaluate()
    .setTitle("EpiGraph PH Dashboard")
    .addMetaTag("viewport", "width=device-width, initial-scale=1")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getBootstrapData() {
  var dashboard = readJsonFile_("dashboard_feed.json") || {};
  var insights = readJsonFile_("insights.json") || {};
  var publication = readJsonFile_("publication_assets.json") || {};
  var summary = readJsonFile_("summary.json") || {};
  var claims = readJsonLinesFile_("claims.jsonl");
  var observations = safeReadJsonLinesFile_("observations.jsonl");
  var reviewQueue = safeReadJsonLinesFile_("review_queue_enriched.jsonl");
  if (!reviewQueue.length) {
    reviewQueue = readJsonLinesFile_("review_queue.jsonl");
  }

  return {
    success: true,
    mode: "apps_script",
    generatedAt: new Date().toISOString(),
    dashboard: dashboard,
    insights: insights,
    publication: publication,
    summary: summary,
    observations: observations,
    filters: buildFilters_(claims, reviewQueue),
  };
}

function searchClaims(request) {
  request = request || {};
  var claims = readJsonLinesFile_("claims.jsonl");
  var query = normalizeText_(request.query || "");
  var limit = sanitizeLimit_(request.limit);
  var filtered = [];

  for (var i = 0; i < claims.length; i++) {
    var row = claims[i];
    if (!matchesClaimFilters_(row, request, query)) {
      continue;
    }
    filtered.push(formatClaimResult_(row));
  }

  filtered.sort(compareClaims_);

  return {
    success: true,
    total: filtered.length,
    results: filtered.slice(0, limit),
  };
}

function searchReviewQueue(request) {
  request = request || {};
  var rows = safeReadJsonLinesFile_("review_queue_enriched.jsonl");
  if (!rows.length) {
    rows = readJsonLinesFile_("review_queue.jsonl");
  }
  var query = normalizeText_(request.query || "");
  var limit = sanitizeLimit_(request.limit);
  var filtered = [];

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    if (!matchesReviewFilters_(row, request, query)) {
      continue;
    }
    filtered.push(formatReviewResult_(row));
  }

  filtered.sort(compareReviewQueue_);

  return {
    success: true,
    total: filtered.length,
    results: filtered.slice(0, limit),
  };
}

function sanitizeLimit_(limit) {
  var parsed = parseInt(limit, 10);
  if (!parsed || parsed < 1) {
    return DEFAULT_LIMIT;
  }
  return Math.min(parsed, 200);
}

function matchesClaimFilters_(row, request, normalizedQuery) {
  if (request.chartReadyOnly && !truthy_(row.chart_ready)) {
    return false;
  }
  if (!matchesExact_(row.primary_disease, request.disease)) {
    return false;
  }
  if (!matchesExact_(row.document_type, request.documentType)) {
    return false;
  }
  if (!matchesExact_(row.metric_type, request.metricType)) {
    return false;
  }
  if (!matchesExact_(row.category, request.category)) {
    return false;
  }
  if (!matchesExact_(String(row.year || ""), String(request.year || ""))) {
    return false;
  }
  if (!normalizedQuery) {
    return true;
  }

  var haystack = normalizeText_(
    [
      row.claim_text,
      row.snippet,
      row.filename,
      row.metric_type,
      row.category,
      row.primary_disease,
      row.region,
      row.row_label,
    ].join(" ")
  );
  return haystack.indexOf(normalizedQuery) !== -1;
}

function matchesReviewFilters_(row, request, normalizedQuery) {
  if (!matchesExact_(row.review_reason, request.reviewReason)) {
    return false;
  }
  if (!matchesExact_(row.priority, request.priority)) {
    return false;
  }
  if (!matchesExact_(row.document_type, request.documentType)) {
    return false;
  }
  if (!matchesExact_(row.metric_type, request.metricType)) {
    return false;
  }
  if (!matchesExact_(row.primary_disease, request.disease)) {
    return false;
  }
  if (!matchesExact_(String(row.year || ""), String(request.year || ""))) {
    return false;
  }
  if (!normalizedQuery) {
    return true;
  }

  var haystack = normalizeText_(
    [
      row.claim_text,
      row.snippet,
      row.filename,
      row.review_reason,
      row.metric_type,
      row.notes,
    ].join(" ")
  );
  return haystack.indexOf(normalizedQuery) !== -1;
}

function matchesExact_(actual, expected) {
  if (!expected || expected === "all") {
    return true;
  }
  return String(actual || "") === String(expected);
}

function truthy_(value) {
  return value === true || value === "True" || value === "true" || value === 1 || value === "1";
}

function compareClaims_(a, b) {
  if (a.chart_ready !== b.chart_ready) {
    return a.chart_ready ? -1 : 1;
  }
  if (Number(a.year || 0) !== Number(b.year || 0)) {
    return Number(b.year || 0) - Number(a.year || 0);
  }
  return Number(b.confidence || 0) - Number(a.confidence || 0);
}

function compareReviewQueue_(a, b) {
  var priorityOrder = { high: 0, medium: 1, low: 2 };
  var aPriority = priorityOrder[String(a.priority || "").toLowerCase()];
  var bPriority = priorityOrder[String(b.priority || "").toLowerCase()];
  aPriority = typeof aPriority === "number" ? aPriority : 3;
  bPriority = typeof bPriority === "number" ? bPriority : 3;
  if (aPriority !== bPriority) {
    return aPriority - bPriority;
  }
  if (Number(a.year || 0) !== Number(b.year || 0)) {
    return Number(b.year || 0) - Number(a.year || 0);
  }
  return Number(b.confidence || 0) - Number(a.confidence || 0);
}

function formatClaimResult_(row) {
  return {
    claim_id: row.claim_id || "",
    filename: row.filename || "",
    document_type: row.document_type || "",
    category: row.category || "",
    metric_type: row.metric_type || "",
    primary_disease: row.primary_disease || "",
    region: row.region || "",
    period_label: row.period_label || "",
    year: row.year || "",
    confidence: Number(row.confidence || 0),
    chart_ready: truthy_(row.chart_ready),
    observation_count: Number(row.observation_count || 0),
    row_label: row.row_label || "",
    claim_text: row.claim_text || "",
    snippet: row.snippet || "",
    source_url: row.source_url || "",
    page_index: row.page_index || "",
  };
}

function formatReviewResult_(row) {
  return {
    review_id: row.review_id || "",
    filename: row.filename || "",
    document_type: row.document_type || "",
    review_reason: row.review_reason || "",
    priority: row.priority || "",
    metric_type: row.metric_type || "",
    primary_disease: row.primary_disease || "",
    region: row.region || "",
    period_label: row.period_label || "",
    year: row.year || "",
    confidence: Number(row.confidence || 0),
    notes: row.notes || "",
    proposed_action: row.proposed_action || "",
    template_family: row.template_family || "",
    claim_text: row.claim_text || "",
    snippet: row.snippet || "",
    source_url: row.source_url || "",
    page_index: row.page_index || "",
  };
}

function buildFilters_(claims, reviewQueue) {
  return {
    claimCategories: counterToOptions_(countBy_(claims, "category")),
    documentTypes: counterToOptions_(countBy_(claims, "document_type")),
    metricTypes: counterToOptions_(countBy_(claims, "metric_type")),
    diseases: counterToOptions_(countBy_(claims, "primary_disease")),
    years: counterToOptions_(countBy_(claims, "year"), true),
    reviewReasons: counterToOptions_(countBy_(reviewQueue, "review_reason")),
    reviewPriorities: counterToOptions_(countBy_(reviewQueue, "priority")),
  };
}

function countBy_(rows, key) {
  var counts = {};
  for (var i = 0; i < rows.length; i++) {
    var value = String(rows[i][key] || "").trim();
    if (!value) {
      continue;
    }
    counts[value] = (counts[value] || 0) + 1;
  }
  return counts;
}

function counterToOptions_(counts, numericSort) {
  var keys = Object.keys(counts);
  keys.sort(function (a, b) {
    if (numericSort) {
      return Number(b) - Number(a);
    }
    return a.localeCompare(b);
  });
  return keys.map(function (key) {
    return { value: key, count: counts[key] };
  });
}

function normalizeText_(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function readJsonFile_(filename) {
  var text = readTextFile_(filename, true);
  if (!text) {
    return null;
  }
  return JSON.parse(text);
}

function readJsonLinesFile_(filename) {
  var text = readTextFile_(filename, false);
  if (!text) {
    return [];
  }
  var rows = [];
  var lines = text.split(/\r?\n/);
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (!line) {
      continue;
    }
    rows.push(JSON.parse(line));
  }
  return rows;
}

function safeReadJsonLinesFile_(filename) {
  try {
    return readJsonLinesFile_(filename);
  } catch (error) {
    return [];
  }
}

function readTextFile_(filename, allowCache) {
  ensureConfigured_();

  var cacheKey = "epigraph:" + filename;
  if (allowCache) {
    var cached = CacheService.getScriptCache().get(cacheKey);
    if (cached) {
      return cached;
    }
  }

  var folder = DriveApp.getFolderById(DATA_FOLDER_ID);
  var files = folder.searchFiles("title = '" + filename + "' and trashed = false");
  if (!files.hasNext()) {
    throw new Error("Drive file not found: " + filename);
  }

  var file = files.next();
  var content = file.getBlob().getDataAsString();
  if (allowCache && content.length <= SMALL_FILE_CACHE_MAX_CHARS) {
    CacheService.getScriptCache().put(cacheKey, content, SMALL_FILE_CACHE_SECONDS);
  }
  return content;
}

function ensureConfigured_() {
  if (!DATA_FOLDER_ID || DATA_FOLDER_ID === "YOUR_DRIVE_FOLDER_ID_HERE") {
    throw new Error("Set DATA_FOLDER_ID in Code.gs before deploying the web app.");
  }
}
