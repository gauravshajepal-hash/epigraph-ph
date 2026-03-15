-- EpicGraph PH Canonical Schema

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT,
    file_hash TEXT UNIQUE NOT NULL,
    local_path TEXT NOT NULL,
    
    -- State machine: 'downloaded', 'parsing', 'parsed', 'extracting', 'extracted',
    -- 'verifying', 'verified', 'synced', 'failed'
    status TEXT DEFAULT 'downloaded',
    claimed_by TEXT,
    claimed_at TIMESTAMP,
    parse_checkpoint_page INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER,
    
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used TEXT NOT NULL,
    documents_processed INTEGER DEFAULT 0,
    is_complete BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS knowledge_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    
    claim_text TEXT NOT NULL,
    category TEXT NOT NULL,
    value TEXT,
    
    source_url TEXT,
    page_index INTEGER NOT NULL,
    snippet_start_offset INTEGER NOT NULL,
    snippet_end_offset INTEGER NOT NULL,
    snippet TEXT NOT NULL,
    confidence FLOAT NOT NULL,
    
    -- For data versioning (if re-extracted)
    superseded_by_run_id INTEGER,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY(document_id) REFERENCES documents(id),
    FOREIGN KEY(run_id) REFERENCES runs(id),
    FOREIGN KEY(superseded_by_run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    knowledge_point_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE,
    FOREIGN KEY(knowledge_point_id) REFERENCES knowledge_points(id)
);

-- Full-Text Search Virtual Table
CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_fts USING fts5(
    claim_text,
    category,
    snippet,
    content='knowledge_points',
    content_rowid='id'
);

-- Performance indices
CREATE INDEX IF NOT EXISTS idx_kp_doc ON knowledge_points(document_id);
CREATE INDEX IF NOT EXISTS idx_kp_category ON knowledge_points(category);
CREATE INDEX IF NOT EXISTS idx_kp_confidence ON knowledge_points(confidence);
CREATE INDEX IF NOT EXISTS idx_doc_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_doc_status ON documents(status);

-- Database Triggers for auditing
CREATE TRIGGER IF NOT EXISTS update_docs_timestamp
AFTER UPDATE ON documents
BEGIN
    UPDATE documents SET last_updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
