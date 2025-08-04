import program_setup
import cdl_osti_db_functions as cdl
import elink_2_functions as elink_2
from pprint import pprint
from time import sleep


# =======================================
def main():
    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    pprint(creds)
    pprint(creds['cdl_db_read'])

    # Get the null DOIs from the CDL DB
    cdl_submissions_without_dois = cdl.get_cdl_pubs_without_dois(creds['cdl_db_read'])

    print(f"{len(cdl_submissions_without_dois)} pubs without DOIs on our end"
          f" to query from OSTI.")

    for item in cdl_submissions_without_dois:
        sleep(1)
        print(f"\n----------------")
        print(f"Elements ID: {item['elements_id']}")
        print(f"eSchol ID: {item['eschol_id']}")
        print(f"OSTI ID: {item['osti_id']}")
        print("Querying E-Link 2...")

        try:
            response = elink_2.get_single_pub(creds['osti_api'], item['osti_id'])
            osti_pub = response.json()
        except Exception as e:
            print(e)
            print("Request or JSON decode error. Skipping...")
            continue

        if osti_pub.get('doi') is None:
            print("OSTI pub has no doi. Skipping...")
            continue
        elif osti_pub['doi'] is None or osti_pub['doi'] == 'None':
            print("OSTI pub has no doi. Skipping...")
            continue
        else:
            print(f"OSTI DOI found: {osti_pub['doi']}")
            print("Updating CDL DB with OSTI DOI.")
            cdl.update_with_osti_doi(
                creds['cdl_db_write'], item['osti_id'], osti_pub['doi'])


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
