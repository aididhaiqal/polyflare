-- provider model priority pricing
--
-- Forward-only. Priority/fast service tiers are billed at a different (higher) rate than standard,
-- but a custom provider model could only carry ONE set of rates, so a priority turn was costed at
-- the standard price. These columns hold the priority-tier rates.
--
-- NULL means "this model has no separate priority rate", which is the correct reading for every
-- pre-existing row: cost then falls back to the standard rates exactly as it does today, so no
-- historical or current row changes price.
--
-- Billing selects between the two using the tier the UPSTREAM REPORTED on the response, never the
-- tier we asked for — a provider may accept a priority request and still serve it as standard, and
-- charging the premium for a turn that was not actually prioritised would overstate cost.
ALTER TABLE provider_models ADD COLUMN priority_input_per_million REAL;
ALTER TABLE provider_models ADD COLUMN priority_cached_input_per_million REAL;
ALTER TABLE provider_models ADD COLUMN priority_output_per_million REAL;
