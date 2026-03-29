# CoPilot → HubSpot Field Mapping

---

## ✅ Contact Fields (Synced)

| HubSpot Field | CoPilot Source | Notes |
|---|---|---|
| `firstname` | `ownership.owner.ownerName` (first token) | From OG (oldest) merchant |
| `lastname` | `ownership.owner.ownerName` (remaining) | From OG merchant |
| `email` | `ownership.owner.ownerEmail` | From OG merchant; skipped if duplicate on another contact |
| `phone` | `ownership.owner.ownerPhone` | From OG merchant |
| `mobilephone` | `ownership.owner.ownerMobilePhone` | From OG merchant |
| `address` | `demographic.businessAddress.address1/2` | From **newest** merchant |
| `city` | `demographic.businessAddress.city` | From **newest** merchant |
| `state` | `demographic.businessAddress.stateCd` | From **newest** merchant (PA → Pennsylvania) |
| `zip` | `demographic.businessAddress.zip` | From **newest** merchant |
| `company` | `merchant.dbaName` | Multi: newest→oldest slash-separated |
| `website` | `demographic.websiteAddress` | From OG merchant |
| `platform` | `processing.platformDetails.backEndPlatformCd` | FDNOB → North, etc. |
| `industry` | `processing.platformDetails.mccId` | Multi-checkbox; all merchant MCCs merged |
| `monthly_processing_volume` | `processing.volumeDetails.averageMonthlyVolume` | Multi: summed across all merchants → range |
| `merchant_id` | `processing.platformDetails.backEndMid` | Multi: newest→oldest slash-separated |
| `date_of_birth` | `ownership.owner.ownerDob` | From OG merchant |
| `pricing_type` | `merchant.pricing.*` | Multi-checkbox; all pricing types across merchants |
| `point_of_sale` | Orders → `equipmentCatalog` → HubSpot rules | Multi-select; gateway dropped if other equipment exists |
| `status_2__cloned_` | CoPilot boarding status | Adds `Current Merchant` on LIVE; preserves existing checkboxes |
| `current_processor` | CoPilot boarding status | `CardChamp` on LIVE; blank on cancelled |
| `date_boarded` | `merchantStatus.boardedDatetime` | From OG merchant status (epoch ms) |
| `live_date` | `merchantStatus.liveDatetime` | From OG merchant status (string) |

---

## ✅ Deal Fields (Synced)

One deal per CoPilot merchant ID. Deal name: `DBA Name - copilotId`.

| HubSpot Field | Source | Notes |
|---|---|---|
| `dealname` | `merchant.dbaName + copilot_id` | One deal per merchant |
| `dealstage` | CoPilot boarding/signature status | Interested → Contract Sent → Boarded → Live |
| `amount` | `averageMonthlyVolume` | Set when stage ≥ 6 (Contract Sent+) |
| `closedate` | HubSpot automatic | Set by HubSpot when deal moves to closed/won |
| `hubspot_owner_id` | Inherited from contact owner | Preserved on create |

---

## 📋 Multi-Merchant Ordering

`copilot_account` format: `x / y / z` where **x = newest**, **z = oldest (OG/primary)**

- **Personal fields** (name, email, phone, DOB): OG merchant
- **Address fields**: newest merchant
- **Company / MID / Volume**: combined across all, newest first in display

---

## 🔑 Important IDs

| ID | HubSpot Field | Value Example | Purpose |
|---|---|---|---|
| CoPilot Account # | `copilot_account` | `170761464` | Manually entered; used to look up merchant |
| Processing MID | `merchant_id` | `496615994886` | `backEndMid` — the actual processing account number |

---

## 📊 Data Transformations

| Transform | From | To |
|---|---|---|
| State | `PA` | `Pennsylvania` |
| MCC | `8041` | `8041 - Chiropractors` |
| Volume | `$30,000` | `20-50K` |
| Platform | `FDNOB` | `North` |
| Datetime | `02/21/2023 9:58:59 AM` | epoch ms (for HubSpot datetime fields) |

---

## ⏸ Paused (not yet synced)

| Field | Reason |
|---|---|
| `ach___e_check_provider` | Paused — need to confirm CoPilot ACH source |
| `pci_compliance` | Paused — need client confirmation on compliance logic |
| MTD / YTD Volume | Paused — not present in CoPilot merchant endpoint |
| Last Date Deposit | Blocked — no confirmed CoPilot source |
| Sales Code → Owner | Blocked — needs HubSpot property + hierarchy clarification |

---

## ❓ Open Questions

- **Production Credentials ticket**: CSV says if only CardPointe Gateway + no Production Credentials ticket, default POS to Virtual Terminal. No ticket API integrated yet.
- **Live Date on deal**: Mapped to `closedate` (natural HubSpot equivalent). Confirm this is correct with client.
- **HubSpot email workflow on Live**: Client needs to configure automation in HubSpot to trigger on status change.
