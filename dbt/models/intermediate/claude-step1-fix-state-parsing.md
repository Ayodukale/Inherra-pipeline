# claude.md: Step 1.1 - Refactor State Extraction Logic

## 🎯 Objective
Refactor the SQL logic for extracting `mailing_state` and `property_state` in the `int_r_score_features.sql` dbt model. The new logic must be significantly more robust, using regex to minimize 'UNKNOWN' results. This is the first step in our "Finalize Scoring" project.

## 📚 Context Files
- `int_r_score_features.txt` (the current, brittle implementation)
- `scoring_logic.md` (for business rule R12.5 and the duplicate R27 for out-of-state)
- `architecture_overview.md` (to understand data flow)

## ✅ Acceptance Criteria
1.  The final code replaces the old `CASE` statements for `mailing_state` and `property_state`.
2.  The new logic uses `COALESCE` and `REGEXP_SUBSTR` to reliably find a two-letter state code.
3.  The logic handles address formats like `City, ST 12345` and `City ST 12345`.
4.  The SQL is well-commented.

## 💡 Implementation Hint
Use `COALESCE` to try multiple regex patterns in order of reliability:
1. First, look for a state code after a comma (e.g., `, TX `).
2. If that fails, look for a state code surrounded by spaces (e.g., ` TX `).
3. If all else fails, default to 'UNKNOWN'.

Please generate the complete, refactored `int_r_score_features.sql` file.