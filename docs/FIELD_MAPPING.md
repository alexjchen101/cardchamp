# CoPilot → HubSpot mapping

Single source of truth for what the sync scripts read and write.

## HubSpot input

| Property | Format |
|----------|--------|
| `copilot_account` | One field. Multiple CoPilot merchant IDs: **`newest / … / oldest`** separated by **`/`** (spaces optional). Scripts parse oldest→OG first internally. |

No `copilot_account_1…6` — retired; use only `copilot_account`.

## Contact fields (synced)

| HubSpot | CoPilot source | Notes |
|---------|----------------|--------|
| `firstname`, `lastname` | `ownership.ownerName` | OG merchant; title case per word |
| `email` | `ownership.ownerEmail` | OG; skipped if duplicate on another contact |
| `phone`, `mobilephone` | `ownership.ownerPhone`, `ownerMobilePhone` | OG |
| `address`, `city`, `state`, `zip` | `demographic.businessAddress` | **Newest** merchant; street/city title-cased |
| `company` | `merchant.dbaName` | Title case per word; multi: slash-separated newest→oldest |
| `website` | `demographic.websiteAddress` | OG merchant in `map_copilot_to_hubspot` |
| `platform` | `processing.platformDetails.backEndPlatformCd` | e.g. FDNOB → North |
| `industry_mcc` | `processing.platformDetails.mccId` | Multi-checkbox; all merchants merged |
| `monthly_processing_volume` | `volumeDetails.averageMonthlyVolume` | Multi: sum → range |
| `merchant_id` | `backEndMid` | Multi: slash-separated newest→oldest |
| `date_of_birth` | `ownership.ownerDob` | OG |
| `pricing_type` | `merchant.pricing.*` | HubSpot values: Flat Rate, IC Plus, Swiped/Non-Swiped |
| `point_of_sale` | Orders + `equipmentCatalog` + rules | Multi-select; CoPilot-derived options are **merged** into existing HubSpot values. Mapping rules live in ``field_mappings.py`` (embedded labels + ordered predicates). See **[POINT_OF_SALE_MAPPING.md](POINT_OF_SALE_MAPPING.md)** for how to extend mappings. Unknowns resolve to **Other**; **CardPointe Gateway** is omitted if any other equipment line exists on that merchant |
| `ach___e_check_provider` (`ACH Provider`) | `processing.blueChexSecOptions`, `processing.blueChexSecVolume` | If BlueChex / ACH From Fiserv is present, set to `Fiserv ACH`; otherwise leave blank |
| `sales_code` | `merchant.salesCode` | OG contact; deal too if `sales_code` exists on deals |
| `hubspot_owner_id` | sales-code owner map | Primary: `data/owner_mapping.csv`. If missing/empty: `data/legacy/sales_code_owner_map.json`. Inactive/cancelled codes (no owner on the row) do not assign an owner |
| `status_2__cloned_` | Boarding status | Merchant lifecycle values are mutually exclusive: pre-live uses `Potential Merchant`, boarded/live uses `Current Merchant` (HubSpot label: Customer); preserve other roles |
| `current_processor` | Status | `CardChamp` on LIVE; clear on cancel (`sync_with_status` only) potential |
| `date_boarded`, `live_date` | `merchantStatus` (OG status payload) | Epoch ms / string |

## Deal fields

One deal per CoPilot ID. Name: `{DBA} - {copilotId}`.

| HubSpot | Source |
|---------|--------|
| `dealstage` | LIVE → Live; BOARDED → Boarded; signature SENT/PENDING/SIGNED → Contract Sent; else Interested |
| `amount` | Approx. monthly volume when stage ≥ Contract Sent |
| `hubspot_owner_id` | sales-code owner map / current contact owner fallback |
| `closedate` | Not set by us — HubSpot default on closed-won |

## Still unresolved

| Item | Reason |
|------|--------|
| PCI, MTD/YTD, last deposit | Source or client rules TBD |
| Production Credentials ticket → POS | No ticket API |

## Reference

Original client field list: `docs/reference/CoPilot - HubSpot Data Flow - Field Mapping.csv`.
