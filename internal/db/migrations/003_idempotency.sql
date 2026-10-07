-- NULL keys do not collide: two jobs submitted without a key are independent.
-- A non-NULL key identifies one logical submission.
ALTER TABLE jobs ADD COLUMN idempotency_key text UNIQUE;
