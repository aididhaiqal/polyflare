-- Terminal outcomes must outlive the account row. An account can be removed while an upstream
-- consume is in flight, and losing the result would turn a known terminal response back into an
-- ambiguous operation on retry.
CREATE TABLE reset_credit_redeem_results (
    account_id TEXT NOT NULL,
    redeem_request_id TEXT NOT NULL,
    result_code TEXT NOT NULL,
    windows_reset INTEGER NOT NULL,
    redeemed_at INTEGER,
    completed_at INTEGER NOT NULL,
    PRIMARY KEY (account_id, redeem_request_id)
);

CREATE INDEX idx_reset_credit_redeem_results_completed
    ON reset_credit_redeem_results(completed_at);

INSERT INTO reset_credit_redeem_results (
    account_id, redeem_request_id, result_code, windows_reset, redeemed_at, completed_at
)
SELECT account_id, redeem_request_id, result_code, windows_reset, redeemed_at, completed_at
FROM reset_credit_redeem_requests
WHERE result_code IS NOT NULL
  AND windows_reset IS NOT NULL
  AND completed_at IS NOT NULL;

-- Older native operations stored their terminal outcome only in the account-scoped ledger.
-- Backfill the native row in the same migration so deleting that account after upgrade cannot
-- strand an otherwise completed stock-Codex request.
UPDATE reset_credit_native_requests
SET result_code = (
        SELECT result.result_code
        FROM reset_credit_redeem_results AS result
        WHERE result.account_id = reset_credit_native_requests.account_id
          AND result.redeem_request_id = reset_credit_native_requests.redeem_request_id
    ),
    windows_reset = (
        SELECT result.windows_reset
        FROM reset_credit_redeem_results AS result
        WHERE result.account_id = reset_credit_native_requests.account_id
          AND result.redeem_request_id = reset_credit_native_requests.redeem_request_id
    ),
    completed_at = (
        SELECT result.completed_at
        FROM reset_credit_redeem_results AS result
        WHERE result.account_id = reset_credit_native_requests.account_id
          AND result.redeem_request_id = reset_credit_native_requests.redeem_request_id
    )
WHERE result_code IS NULL
  AND EXISTS (
      SELECT 1
      FROM reset_credit_redeem_results AS result
      WHERE result.account_id = reset_credit_native_requests.account_id
        AND result.redeem_request_id = reset_credit_native_requests.redeem_request_id
  );
