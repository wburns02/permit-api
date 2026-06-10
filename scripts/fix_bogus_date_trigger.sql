-- ============================================================================
-- fix_bogus_date_trigger.sql
-- ----------------------------------------------------------------------------
-- Replaces public.reject_bogus_issue_date() so that bogus issue_date values
-- are quarantined (set to NULL) on INSERT instead of raising and aborting
-- the entire batch.
--
-- Trigger binding (hot_leads_reject_bogus_issue_date on hot_leads) is left
-- intact -- we only CREATE OR REPLACE the underlying function.
--
-- The original function guarded a single column: issue_date.
-- Range: [2000-01-01, CURRENT_DATE + 5 years].
-- We preserve the same range and the same single-column scope (do not
-- expand to applied_date etc. -- the original did not guard them, and
-- broadening the contract here is out of scope for this fix).
--
-- Date: 2026-05-15
-- Author: trigger-fix task
-- ============================================================================

CREATE OR REPLACE FUNCTION public.reject_bogus_issue_date()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    v_min CONSTANT DATE := DATE '2000-01-01';
    v_max DATE := (CURRENT_DATE + INTERVAL '5 years')::DATE;
BEGIN
    IF NEW.issue_date IS NOT NULL
       AND (NEW.issue_date < v_min OR NEW.issue_date > v_max) THEN
        -- Low-frequency NOTICE so we have *some* visibility without log spam.
        -- Fires ~1 of every 500 quarantines (random sample).
        IF random() < 0.002 THEN
            RAISE NOTICE
                'reject_bogus_issue_date: quarantined issue_date=% on source=% jurisdiction=% permit_number=% (set NULL)',
                NEW.issue_date, NEW.source, NEW.jurisdiction, NEW.permit_number;
        END IF;
        NEW.issue_date := NULL;
    END IF;
    RETURN NEW;
END;
$function$;
