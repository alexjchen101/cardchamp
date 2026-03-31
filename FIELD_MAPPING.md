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
| `point_of_sale` | Orders + `equipmentCatalog` + rules | Multi-select; **CardPointe Gateway** omitted if any other line on that merchant |
| `sales_code` | `merchant.salesCode` | OG contact; deal too if `sales_code` exists on deals |
| `status_2__cloned_` | Boarding status | On LIVE: add `Current Merchant`, remove `Potential Merchant`; merge preserves other roles |
| `current_processor` | Status | `CardChamp` on LIVE; clear on cancel (`sync_with_status` only) potential |
| `date_boarded`, `live_date` | `merchantStatus` (OG status payload) | Epoch ms / string |

## Deal fields

One deal per CoPilot ID. Name: `{DBA} - {copilotId}`.

| HubSpot | Source |
|---------|--------|
| `dealstage` | LIVE → Live; BOARDED → Boarded; signature SENT/PENDING/SIGNED → Contract Sent; else Interested |
| `amount` | Approx. monthly volume when stage ≥ Contract Sent |
| `closedate` | Not set by us — HubSpot default on closed-won |

## Paused / blocked

| Item | Reason |
|------|--------|
| ACH, PCI, MTD/YTD, last deposit | Source or client rules TBD |
| Sales code → owner | Hierarchy TBD |
| Production Credentials ticket → POS | No ticket API |

## Client CSV

See `CoPilot - HubSpot Data Flow - Field Mapping.csv` for original requirements.
