# Notes

## ⚠️ Uncertainties

### 1. ACH / E-Check Provider

- **CoPilot Field:** `processing.blueChexSecOptions`
- **Question:** Is this actually Fiserv ACH?
- **Test merchant:** null

### 2. Cash Discount (Flat Rate Pricing)

- **CoPilot Field:** `pricing.flatPricing`
- **Can extract:** Yes/No
- **Issue:** No HubSpot field yet

### 3. PCI Non Compliance

- **CoPilot Fields:** `fees.pciProgramCd`, `fees.pciAnnualFee`
- **Question:** What indicates non-compliance?
- **Issue:** No HubSpot field yet

---

## 📋 Multi-Business (Multiple CoPilot IDs per Contact)

**Supported input formats:**
1. `copilot_account` – slash-separated: `168407650 / 168406070 / 168407620`
2. `copilot_account_1`, `copilot_account_2`, … `copilot_account_6` – numbered fields

**Order:** Last entry = primary (shared fields). Add new at front: `new / x / y / z` so primary stays.

**Flow:**
- For each CoPilot ID: fetch merchant, create/update deal
- Contact shared fields (name, address, etc.) from first business
- For 2+ businesses: numbered fields `customer_id_1`, `merchant_id_1`, `company_1`, etc.

**Multi-business fields:**
- `company` = slash-separated: "Preston A / Preston B / Preston C"
- `merchant_id` = first business
- `monthly_processing_volume` = **sum** of all businesses → range

---

## ✅ Contact Status Logic

**Field:** `status_2__cloned_` (HubSpot Contact "Status" field)

**Initial Setup:**

- Sets to "Potential Merchant"

**Status-Aware Sync:**

- LIVE → "Current Merchant"
- Other → no change

---

## 📊 Output Format

All scripts show:

- Raw CoPilot data (including empty/null fields)
- HubSpot before values
- What we're sending to HubSpot
- What changed (NEW/CHANGED)

