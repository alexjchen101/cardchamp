# Notes

**Mapping detail →** [FIELD_MAPPING.md](FIELD_MAPPING.md)

**Run sync (production-style):**  
`python3 sync_with_status.py <contact_email>`  
CoPilot IDs come from HubSpot `copilot_account` only (`newest / older / oldest` with `/`).

**Bootstrap / force Interested:**  
`python3 sync_initial_setup.py <contact_email>` (testing; resets deal to Interested).

**Bulk (“all contacts with CoPilot #”):** not implemented yet — extend `HubSpotClient.search_contacts_with_copilot_id()` with pagination, then call `sync_with_status` per contact.
