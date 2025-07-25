import program_setup
import elink_2_functions as elink_2
from pprint import pprint


# =======================================
def main():
    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    sv_response = elink_2.get_pubs_by_workflow_status(creds['osti_api'], 'SV')
    hidden_response = elink_2.get_hidden_pubs(creds['osti_api'])

    try:
        sv_pubs = sv_response.json()
        hidden_pubs = hidden_response.json()

        if not sv_pubs:
            print("No 'SV' status pubs since 2024-10-01.")
        else:
            sv_pubs = [filter_pub(pub) for pub in sv_pubs]
            print_workflow_pub_summaries(sv_pubs)

        if not hidden_pubs:
            print("No hidden pubs since 2024-10-01.")
        else:
            print_hidden_pub_summaries(hidden_pubs)

        if sv_pubs:
            print_workflow_pub_details(sv_pubs)

    except Exception as e:
        raise "An error occured while decoding the API's reponse."


def filter_pub(pub):
    keep_keys = [
        'doi', 'title', 'identifiers',
        'osti_id', 'date_metadata_added',
        'product_type', 'publication_date',
        'released_to_osti_date', 'source_input_type',
        'source_edit_type', 'hidden_flag',
        'audit_logs']

    filtered_pub = {key: pub[key]
                    for key in keep_keys
                    if key in pub}

    return filtered_pub


def print_workflow_pub_summaries(pubs):
    print(f"{len(pubs)} pubs at OSTI with 'SV' workflow status since 2024-10-01.\n"
          f"\nSummary:")

    for pub in pubs:
        print(f"\nOSTI ID: {pub['osti_id']}")
        print(f"OSTI URL: https://www.osti.gov/elink/record/{pub['osti_id']}")
        print(f"DOI: {pub['doi']}")
        eschol_url = ','.join(
            [i['value'] for i in pub['identifiers']
             if 'https://escholarship.org/uc/item/' in i['value']])
        print(f"eSchol URL found in 'identifiers': {eschol_url}")
        print(f"site_url: {pub.get('site_url')}")
        file_urls = get_file_urls(pub)
        print(f"file array URLs: {file_urls}")

        last_log = pub['audit_logs'][-1]
        print(f"Most recent audit log:")
        pprint(last_log)


def print_workflow_pub_details(pubs):
    print("\n\n----------------")
    print("'SV' pubs, more details:")

    for pub in pubs:
        print(f"\n\nDOI: {pub['doi']}")
        pprint(pub)


def print_hidden_pub_summaries(pubs):
    print("\n\n-----------------------------------")
    print(f"{len(pubs)} hidden publications since 2024-10-01.\n")
    print("Sensitivity flags: ")
    print("""U : Unlimited distribution
S : Sensitive, no public distribution
H : Hybrid sensitivity
E : Media is under embargo
X : No distribution, usually error condition
    """)
    for pub in pubs:
        print(f"\nOSTI ID: {pub['osti_id']}")
        print(f"OSTI URL: https://www.osti.gov/elink/record/{pub['osti_id']}")
        print(f"DOI: {pub['doi']}")
        eschol_url = ','.join(
            [i['value'] for i in pub['identifiers']
             if 'https://escholarship.org/uc/item/' in i['value']])
        print(f"eSchol URL found in 'identifiers': {eschol_url}")
        print(f"site_url: {pub.get('site_url')}")
        print(f"sensitivity_flag: {pub.get('sensitivity_flag')}")


def get_file_urls(p):
    media = p.get('media')
    if not media:
        return None

    file_urls = []
    for m in media:
        files = m.get('files')
        for f in files:
            furl = f.get('url')
            if furl:
                file_urls.append(furl)

    if not file_urls:
        return None
    else:
        return ','.join(file_urls)


# =======================================
# Stub for main
if __name__ == "__main__":
    main()
