# Notes

---

## 📋 Multi-Merchant (Multiple CoPilot IDs per Contact)

**Input format:** `copilot_account` = slash-separated, newest first: `x / y / z`
- `x` = newest merchant, `z` = oldest (OG/primary)
- Add new IDs to the front — oldest stays at the end automatically

**Field precedence:**
- Personal fields (name, email, phone, DOB) → **OG** merchant
- Address fields (address, city, state, zip) → **newest** merchant
- Company / MID → slash-separated newest→oldest
- Volume → **sum** across all merchants → range
- Industry, Pricing Type, Point of Sale → **merged** across all merchants

---

## 🔄 Sync Scripts

### `sync_initial_setup.py`
- Status-blind: always sets deal stage to "Interested", contact status to "Potential Merchant"
- Use for: initial setup, testing

### `sync_with_status.py`
- Status-aware: sets deal stage from CoPilot boarding status, sets `Current Merchant` + `CardChamp` on LIVE
- Use for: ongoing/daily sync

---

## 🏷 Contact Status Logic

**Field:** `status_2__cloned_`

| CoPilot Status | Action |
|---|---|
| Initial setup | Add `Potential Merchant` (preserve others) |
| LIVE | Add `Current Merchant` (preserve others) |
| Other | No change |

Existing checkboxes (e.g. Sales Agent, Brand Ambassador) are always preserved.

---

## 📦 Point of Sale / Gateway (`point_of_sale`)

**Source:** `GET /order/list` per merchant → `equipmentCatalog` → HubSpot rules

**Gateway rule (per merchant):**
- Only gateway orders → map to `CardPointe Virtual Terminal`
- Gateway + other equipment → **drop** gateway; map only terminals/other hardware
- Multi-merchant: rule runs per merchant, results merged (semicolon-joined values)

**Matching:** Ordered rule table in `field_mappings._hubspot_pos_rules()`. Loads HubSpot option schema to validate values. Extend rules as new CoPilot catalog strings are seen.

---

## 📅 Date Fields

| HubSpot Field | Source | Notes |
|---|---|---|
| `date_boarded` (contact) | `merchantStatus.boardedDatetime` | OG merchant |
| `live_date` (contact) | `merchantStatus.liveDatetime` | OG merchant |
| `closedate` (deal) | HubSpot automatic | Set by HubSpot when deal stage moves to Live/closed |
| Deal stage → Boarded | `merchantStatus.boardedDatetime` | Stage logic handles this automatically |

---

## ⚠️ Known Gaps / Pending

| Item | Status |
|---|---|
| ACH Provider | Paused |
| PCI Compliance | Paused — need client's compliance logic |
| MTD / YTD Volume | Paused — not in CoPilot endpoint |
| Last Date Deposit | Blocked — no CoPilot source found |
| Sales Code → Contact/Deal Owner | Blocked — needs HubSpot property + hierarchy clarification |
| Production Credentials ticket → POS | Blocked — no ticket API |
| HubSpot email workflow on Live | Client task — configure in HubSpot automation |
