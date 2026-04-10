-- migration 0007: add rerun_count to review_queue
-- Tracks how many times a listing has been sent back to the AI for re-processing.
ALTER TABLE public.review_queue ADD COLUMN IF NOT EXISTS rerun_count SMALLINT NOT NULL DEFAULT 0;
