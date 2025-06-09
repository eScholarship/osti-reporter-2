# Transform for OSTI E-Link Version 2
# https://review.osti.gov/elink2api/#tag/records/operation/submitRecord

import json
import release_info
from copy import deepcopy


# ---------------------
# For each new publication, create the JSON that's sent as the HTTP req body.
def add_osti_data(new_osti_pubs, testing_mode):
    print("Converting SQL results into JSON for E-Link v2.")

    # Main loop
    new_osti_pubs_with_json = []
    for pub in new_osti_pubs:

        # Create the pub dict, init with hardcoded release fields.
        # Note: deepcopy is required for cloning nested dicts
        osti_pub = deepcopy(release_info.v2)

        # Translate Elements pub type to OSTI product_type & adds associated metadata
        osti_pub.update(get_product_type_and_subfields(pub))

        # All publications should have these
        osti_pub['title'] = pub['title']
        osti_pub['site_unique_id'] = pub['id']
        osti_pub['publication_date'] = pub['Reporting Date 1']
        osti_pub['product_size'] = pub['File Size']

        # Some pubs may not have these
        if pub['doi'] is not None:
            osti_pub['doi'] = pub['doi']
        if pub['abstract'] is not None:
            osti_pub['description'] = pub['abstract']

        # Identifiers
        osti_pub['identifiers'].append(dict(type='OTHER_ID', value=pub['ark']))
        osti_pub['identifiers'].append(dict(type='OTHER_ID', value=pub['eSchol URL']))

        # TK Think something's wrong here?
        if get_lbl_report_number(pub) is not None:
            osti_pub['identifiers'].append(dict(type='RN', value=get_lbl_report_number(pub)))

        # Persons
        authors_json = json.loads(pub['authors'])
        if authors_json is not None:

            # Convert email to an array
            for author in authors_json:

                # If the author is an org, append it to the org list.
                if is_organization_author(author):
                    author['org'] = True
                    osti_pub['organizations'].append(format_organization_author(author))

                if 'email' in author.keys():
                    author['email'] = [author['email']]

            # Remove org authors & add to the OSTI persons array.
            authors_json = [author for author in authors_json if 'org' not in author.keys()]
            osti_pub['persons'] += authors_json

        # Grants (These are listed in OSTI Organizations)
        grants_json = json.loads(pub['grants'])
        if grants_json is not None:
            osti_pub['organizations'] += grants_json

        # Save the OSTI submission JSON & add the pub to the list
        pub['submission_json'] = osti_pub
        new_osti_pubs_with_json.append(pub)

    return new_osti_pubs_with_json


# ========================================
# Misc. Helper Functions

# Returns OSTI product_type and associated subfields
def get_product_type_and_subfields(pub):
    if pub['Type'] == 'Journal article' or pub['Type'] == 'Internet publication':
        return {
            'product_type': 'JA',
            'journal_type': 'FT',
            'journal_name': pub['Journal Name'],
            'volume': pub['volume'],
            'issue': pub['issue']
        }

    elif pub['Type'] == 'Conference papers' or pub['Type'] == 'Poster':
        c_type = 'A' if pub['Type'] == 'Conference papers' else 'O'

        conference_name = pub['name-of-conference'] \
            if pub['name-of-conference'] else pub['Journal Name']

        return {
            'product_type': 'CO',
            'conference_type': c_type,
            'conference_information': conference_name
        }

    elif pub['Type'] == 'Book' or pub['Type'] == 'Chapter':
        return {'product_type': 'B'}

    elif pub['Type'] == 'Report':
        return {'product_type': 'TR'}

    else:  # Safety return, should never be reached.
        return None


# -----------------
# Returns the report number based on content
# Hard-code them to begin with LBNL if they don't already.
def get_lbl_report_number(pub):
    if pub['LBL Report Number'] is None:
        return "None"
    elif pub['LBL Report Number'][:5] == 'LBNL-':
        return pub['LBL Report Number']
    else:
        return "LBNL-" + pub['LBL Report Number']


# -----------------
# Check known organizational authors
def is_organization_author(author):
    if ('collaboration' in author['last_name'].lower()
            or ('first_name' in author.keys() and 'collaboration' in author['first_name'].lower())):
        return True
    else:
        return False


# -----------------
# Returns a reasonable format for authoring organization strings.
def format_organization_author(author):
    org_name = author['last_name']
    if 'first_name' in author.keys() and author['first_name'] != ".":
        org_name = author['first_name'] + " " + org_name
    return {"name": org_name, "type": "AUTHOR"}
