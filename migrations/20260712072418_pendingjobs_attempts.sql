-- Add migration script here
ALTER TABLE pendingjobs
ADD COLUMN attempts     INT  NOT NULL DEFAULT 0,
ADD COLUMN max_attempts INT  NOT NULL DEFAULT 5,
ADD COLUMN last_error   TEXT;