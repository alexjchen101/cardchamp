# CoPilot → HubSpot Field Mapping

---

## ✅ Syncing (14 Fields)

| # | HubSpot Field | CoPilot Source | Example |
|---|---------------|----------------|---------|
| 1 | `firstname` | `ownership.owner.ownerName` (split) | "Charles" |
| 2 | `lastname` | `ownership.owner.ownerName` (split) | "Simkovich" |
| 3 | `company` | `merchant.dbaName` | "Simkovich Concussion Institute" |
| 4 | `mobilephone` | `ownership.owner.ownerMobilePhone` | "412-716-7757" |
| 5 | `address` | `demographic.businessAddress` | "52 Pine Creek Road" |
| 6 | `city` | `demographic.businessAddress.city` | "Wexford" |
| 7 | `state` | `demographic.businessAddress.stateCd` | "Pennsylvania" (PA→) |
| 8 | `zip` | `demographic.businessAddress.zip` | "15090" |
| 9 | `website` | `demographic.websiteAddress` | "https://simkovichconcussioninstitute.com/" |
| 10 | `platform` | `processing.platformDetails.backEndPlatformCd` | "North" (FDNOB→) |
| 11 | `industry` | `processing.platformDetails.mccId` | "8041 - Chiropractors" |
| 12 | `monthly_processing_volume` | `processing.volumeDetails.averageMonthlyVolume` | "20-50K" ($30K→) |
| 13 | `merchant_id` | `processing.platformDetails.backEndMid` | "496615994886" |
| 14 | `date_of_birth` | `ownership.owner.ownerDob` | "05/01/1961" |

---

## 📋 Multi-Business

When a contact has multiple CoPilot IDs (slash-separated in `copilot_account`: `a / b / c`; last = primary):

| HubSpot Field | Source |
|---------------|--------|
| `company` | Slash-separated: "Name1 / Name2 / Name3" |
| `merchant_id` | First business |
| `monthly_processing_volume` | **Sum** of all businesses → range |

Same `company` field, single entry.

---

## 🚧 Conditional (Ready When Conditions Met)

| HubSpot Field | CoPilot Source | When |
|---------------|----------------|------|
| `phone` | `ownership.owner.ownerPhone` | Only if HubSpot phone is empty |
| `point_of_sale` | `merchant.equipment[]` | When equipment ordered |
| `ach___e_check_provider` | `processing.blueChexSecOptions` | When ACH enabled (if this is ACH?) |

---

## 🔑 Important IDs

**Three different IDs:**

| ID | Value | Purpose |
|----|-------|---------|
| **CoPilot Account #** | 170761464 | Manually entered to FIND merchant |
| **Merchant Account Number** | 496615994886 | Processing MID (synced) |
| **Customer ID** | 16795258 | CoPilot internal ID |

---

## 📋 Data Transformations

| Transform | From | To |
|-----------|------|-----|
| State | "PA" | "Pennsylvania" |
| MCC | 8041 | "8041 - Chiropractors" |
| Volume | $30,000 | "20-50K" |
| Platform | "FDNOB" | "North" |

---

---

## ⚠️ Uncertainties (See NOTES.md)

- **ACH:** Is `blueChexSecOptions` actually Fiserv ACH?
- **Cash Discount:** Can extract from `pricing.flatPricing`, need HubSpot field
- **PCI Compliance:** Unclear what indicates non-compliance
