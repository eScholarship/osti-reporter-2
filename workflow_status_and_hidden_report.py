import program_setup
import elink_2_functions as elink_2
from pprint import pprint


# =======================================
def main2():
    # ---------- GENERAL SETUP
    # Process args; Assign creds based on args; Create the log folder.
    args = program_setup.process_args()
    creds = program_setup.assign_creds(args)

    sv_response = elink_2.get_pubs_by_workflow_status(creds['osti_api'], 'SV')
    sv_pubs = sv_response.json()
    for item in sv_pubs:
        comments_response = elink_2.get_comments(creds['osti_api'], item['osti_id'])
        try:
            item['comments'] = comments_response.json()
        except:
            item['comments'] = None

    hidden_response = elink_2.get_hidden_pubs(creds['osti_api'])
    hidden_pubs = hidden_response.json()
    for item in hidden_pubs:
        comments_response = elink_2.get_comments(creds['osti_api'], item['osti_id'])
        try:
            item['comments'] = comments_response.json()
        except:
            item['comments'] = None

    print(f"{len(sv_pubs) + len(hidden_pubs)} total items found with issues:\n"
          f"• {len(sv_pubs)} pubs with 'SV' status (not yet released),\n"
          f"• {len(hidden_pubs)} hidden pubs.")
    print("\n================================")

    print_item_info("SV Status", sv_pubs)
    print_item_info("Hidden", hidden_pubs)


def print_item_info(problem, pubs):
    for pub in pubs:
        print("\n\n--------------------")
        print(f"\nPROBLEM: {problem}")
        print(f"OSTI ID: {pub['osti_id']}")
        print(f"OSTI URL: https://www.osti.gov/elink/record/{pub['osti_id']}")
        print(f"DOI: https://doi.org/{pub['doi']}")

        eschol_urls = compile_eschol_urls(pub)
        if eschol_urls:
            if len(eschol_urls) == 1:
                print(f"ESCHOL URL: {eschol_urls[0]}")
            else:
                print('\nESCHOL URLS:')
                for url in eschol_urls:
                    print(f"{url}")

        if pub['audit_logs']:
            print("\nAUDIT LOGS:")
            for index, audit in enumerate(pub['audit_logs']):
                print_audit_log(index, audit)

        if pub['comments']:
            print("\nCOMMENTS:")
            for index, comment in enumerate(pub['comments']):
                print_comment(index, comment)


def split_ts(ts):
    ts = ts.split('+')[0]
    ts = ts.split('.')[0]
    ts = ts.replace('T', ' ')
    return ts


def print_comment(index, comment):
    def get_comment_state(s):
        if s == 'I':
            return 'INFO'
        elif s == 'O':
            return 'OPEN ISSUE'
        elif s == 'C':
            return 'CLOSED ISSUE'
        else:
            return '???'

    state = get_comment_state(comment['state'])
    ts = split_ts(comment['date_added'])
    print(f"[{ts}] [{state}] :: {comment['comments'][0]['text']}")


def print_audit_log(index, audit):

    ts = split_ts(audit['audit_date'])
    messages = ';'.join(audit['messages'])

    print(f"[{ts}] [{audit['type']}] [{audit['status']}] :: {messages}")


def compile_eschol_urls(pub):
    eschol_urls = []
    eschol_urls = [i['value'] for i in pub['identifiers']
                   if 'https://escholarship.org/uc/item/' in i['value']]
    eschol_urls.append(pub.get('site_url'))
    eschol_urls += get_file_urls(pub)
    eschol_urls = [u for u in eschol_urls if u is not None]
    eschol_urls = list(set(eschol_urls))
    return eschol_urls


def get_file_urls(p):
    media = p.get('media')
    if not media:
        return []

    file_urls = []
    for m in media:
        files = m.get('files')
        for f in files:
            furl = f.get('url')
            if furl:
                file_urls.append(furl)

    return file_urls


def get_file_urls(p):
    media = p.get('media')
    if not media:
        return []

    file_urls = []
    for m in media:
        files = m.get('files')
        for f in files:
            furl = f.get('url')
            if furl:
                file_urls.append(furl)

    return file_urls


# =======================================
# Stub for main
if __name__ == "__main__":
    main2()
