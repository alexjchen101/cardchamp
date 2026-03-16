# CardChamp CoPilot → HubSpot Integration

Syncs merchant data from CoPilot (FiSurf) to HubSpot CRM.

---

## 🚀 Run Now

```bash
python3 sync_with_status.py test@test.com
```

Script pulls CoPilot Account # from HubSpot automatically.

Simple output format:
- COPILOT RAW DATA (everything pulled, empty = `(empty)` or `(null)`)
- HUBSPOT BEFORE (current values)
- SENDING TO HUBSPOT (what we're updating)
- CHANGES (NEW or CHANGED only)

---

## 📋 Two Scripts

**sync_with_status.py** - Production (use this)
- Detects CoPilot status → sets correct deal stage
- Updates Contact Status/processor when LIVE
- Syncs all 14 fields

**sync_initial_setup.py** - Testing only
- Sets Contact Status to "Potential Merchant"
- Creates deal at "Interested" stage
- Ignores CoPilot status

---

## 📚 Docs

- **`FIELD_MAPPING.md`** - All 14 syncing fields + transformations
- **`NOTES.md`** - 3 uncertainties (ACH, Cash Discount, PCI)

---

## 📊 Output Format

**COPILOT RAW DATA:**
```
stateCd: PA
backEndPlatformCd: FDNOB
mccId: 8041
averageMonthlyVolume: 30000
equipment: (empty)
blueChexSecOptions: (null)
```

**SENDING TO HUBSPOT:**
```
state: Pennsylvania  (PA transformed)
platform: North  (FDNOB transformed)
industry: 8041 - Chiropractors  (8041 transformed)
monthly_processing_volume: 20-50K  ($30K transformed)
```

**CHANGES:**
```
NEW: platform = North
CHANGED: state = '' → 'Pennsylvania'
```

---

## ✅ Working

- 14 contact fields syncing
- Status-aware deal progression (Interested → Contract Sent → Boarded)
- Contact Status: "Potential Merchant" on initial → "Current Merchant" when LIVE
- Current Processor: "CardChamp" when LIVE, blank when CANCELLED
- Duplicate prevention
- Shows ALL fields in output (including empty ones)

---

## 📁 Files

```
sync_with_status.py    - Production sync (status-aware)
sync_initial_setup.py  - Test sync (status-blind)
status_logic.py        - Status detection
field_mappings.py      - Data transformations
mcc_mapping.py         - MCC → Industry mapping

hubspot/client.py      - HubSpot API
copilot/               - CoPilot API
```
