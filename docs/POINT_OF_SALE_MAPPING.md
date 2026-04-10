# Point of sale (`point_of_sale`) — maintaining CoPilot → HubSpot equipment mapping

This document is for anyone (including AI agents) extending how merchant equipment from CoPilot maps to HubSpot contact multi-select **`point_of_sale`**.

## Where the logic lives

- **Single source of truth:** `field_mappings.py`.
- **Embedded HubSpot labels:** tuple `_EMBEDDED_HUBSPOT_POS_LABELS` (~143 strings). These are exact HubSpot checkbox **labels** (not internal values) that were added in a batch. They are **not** the full set of options on the property.
- **Rules:** `_hubspot_pos_rules()` returns a flat ordered list of `(hubspot_label, predicate)`. Predicates take `(s_lower, raw_name, type_upper)` and use substring checks, `gwc()` (alphanumeric-normalized equality), and gateway `type` (`GATEWAY` vs not) — not heavy regex.
- **Resolution order** (see `logical_pos_label_for_equipment`): first matching rule → longest substring on embedded labels (length ≥ 10) → **Other** (with portal-specific resolution).

POS mapping has **one** code path in `field_mappings.py` — no duplicate CSV or old parallel implementation.

## Workflow when equipment or HubSpot options change

This is the **same loop** used to build and validate mappings: **full CoPilot equipment list → compare to live HubSpot `point_of_sale` options → encode matches in `field_mappings.py` (embedded strings for portal-exact names, predicates for abbreviations and ordering)** — then re-export and spot-check.

1. **Refresh exports** (requires `.env` with CoPilot + HubSpot):
   ```bash
   python3 tools/export_pos_mapping_sources.py
   ```
   This creates **`data/pos_analysis/`** (gitignored — large, reproducible JSON). Files:
   - `copilot_equipment_catalog.json` — full CoPilot `equipmentCatalog`
   - `hubspot_point_of_sale_options.json` — live HubSpot labels/values
   - `copilot_equipment_with_logical_pos.json` — each catalog row run through current rules

   The script also **prints drift** to the console (and you infer the same from the JSON files):
   - **HubSpot labels not in the embedded tuple** — new portal checkboxes to add to `_EMBEDDED_HUBSPOT_POS_LABELS` and/or match with predicates.
   - **Embedded tuple labels not returned by HubSpot** — renamed/removed in the portal; update the tuple or rules to match current labels.

2. **Review CoPilot:** Use `copilot_equipment_catalog.json` (and rows in `copilot_equipment_with_logical_pos.json` where `logical_pos_label` is **Other** or wrong) to find `name` / `type` strings that need new or adjusted rules.

3. **Review HubSpot:** Use `hubspot_point_of_sale_options.json` or the portal for **new** checkbox labels. If a label should match specific CoPilot text, add it to **`_EMBEDDED_HUBSPOT_POS_LABELS`** when the match should be **exact** (after alphanumeric normalization), **or** add a **predicate** in `_hubspot_pos_rules()` when CoPilot uses abbreviations or you need ordering (specific before generic).

4. **Edit `field_mappings.py`:**
   - **Exact portal label + stable CoPilot spelling:** extend `_EMBEDDED_HUBSPOT_POS_LABELS` and rely on the generated gwc-exact rules (longest labels sort first for tie-breaking).
   - **Abbreviations or disambiguation:** add `(label, lambda s, r, t: ...)` entries in `_hubspot_pos_rules()` in the right **order** — **specific** before **generic** (e.g. gateway E-com before plain gateway; Clover 2D scanner before generic Clover barcode).

5. **Re-run** `export_pos_mapping_sources.py` and confirm drift/`logical_pos_label` for the equipment you care about.

## Related

- High-level field behavior: `docs/FIELD_MAPPING.md` (row for `point_of_sale`).
