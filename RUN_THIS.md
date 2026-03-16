# Run This

## Command (Email Only)

```bash
python3 sync_with_status.py test@test.com
```

Script automatically pulls CoPilot Account # from HubSpot contact.

---

## Status Logic

**Initial Setup:**
- Contact Status → "Potential Merchant"
- Deal Stage → "Interested" (1)

**Status-Aware (LIVE):**
- Contact Status → "Current Merchant" (displays as "Customer")
- Deal Stage → "Boarded" (7)
- Current Processor → "CardChamp"

**Status-Aware (PENDING_SIGNATURE):**
- Deal Stage → "Contract Sent" (6)

---

## Output Format

- COPILOT RAW DATA (everything, empty = `(empty)` or `(null)`)
- STATUS LOGIC (what will be set based on status)
- HUBSPOT BEFORE (current values)
- SENDING TO HUBSPOT (after transformations)
- CHANGES (NEW/CHANGED only)

---

## Next: Multiple Contacts

```bash
python3 sync_with_status.py test2@test.com
python3 sync_with_status.py test3@test.com
```
