-- Live-editable config overrides (Settings subsystem). Content-free: key/value strings only — never a token/secret.

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT    PRIMARY KEY,
    value      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);
