import json
import release_info
from pprint import pprint


# ========================================
# Transform for OSTI E-Link Version 2

# For each new publication, create the JSON that's sent as the HTTP req body.
def add_osti_data_v2(new_osti_pubs, testing_mode):
    print("Converting SQL results into JSON for E-Link v2.")

    # Main loop
    new_osti_pubs_updated = []
    for pub in new_osti_pubs:

        # Check the pub type and skip if it's not a type we care about
        pub['product_type'] = get_product_type(pub['Type'])
        if pub['product_type'] is None:
            continue

        # Create the pub dict, init with hardcoded items
        osti_pub = release_info.v2

        # ---------------------
        # Publication-specific metadata
        #   • Some items may not have DOIs.
        osti_pub['title'] = pub['title']
        osti_pub['product_type'] = pub['product_type']
        if osti_pub['product_type'] == 'JA':  # TK  temp workaround
            osti_pub['journal_type'] = 'FT'
        if pub['doi'] is not None:
            osti_pub['doi'] = pub['doi']
        osti_pub['site_unique_id'] = pub['id']
        osti_pub['description'] = pub['abstract']
        osti_pub['publication_date'] = pub['Reporting Date 1']
        # osti_pub['opn_document_location'] = pub['eSchol URL'] # for OpenNet only.
        # osti_pub['format_information'] = pub['File Extension'] # Throws an error if included.
        osti_pub['product_size'] = pub['File Size']

        # Identifiers
        # Note: OSTI says ARKS should go in OTHER_ID
        osti_pub['identifiers'].append(
            dict(type='OTHER_ID', value=pub['ark'], title='ARK'))
        osti_pub['identifiers'].append(
            dict(type='OTHER_ID', value=pub['eSchol URL'], title='eScholarship URL'))

        osti_pub['product_type'] = get_product_type(pub['Type'])
        if get_lbl_report_number(pub) is not None:  # Think something's wrong here
            osti_pub['identifiers'].append(dict(type='RN', value=get_lbl_report_number(pub)))

        # Persons
        authors_json = json.loads(pub['authors'])
        if authors_json is not None:

            # Convert email to an array
            for author in authors_json:

                # Check if the author is an org.
                # If so, append it to the org list.
                if is_organization_author(author):
                    author['org'] = True
                    osti_pub['organizations'].append(format_organization_author(author))

                if 'email' in author.keys():
                    author['email'] = [author['email']]

            # Remove the organization authors.
            authors_json = [author for author in authors_json if 'org' not in author.keys()]
            osti_pub['persons'] += authors_json

        # Grants (Organizations)
        grants_json = json.loads(pub['grants'])
        if grants_json is not None:
            osti_pub['organizations'] += grants_json

        # JSON is serialized into string for testing output
        if testing_mode:
            osti_pub = json.dumps(osti_pub, indent=4)

        # Add to the list
        new_osti_pubs_updated.append(osti_pub)

    return new_osti_pubs_updated


# ========================================
# Misc. Helper Functions

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
# Returns fields based on publication type
def get_pub_type_fields(pub):
    pt = pub['Type']
    ptfields = {}

    if pt == 'Journal article':
        pass

    elif pt == 'Report':
        ptfields['product_type'] = 'TR'
        if pub['File Extension'] == 'pdf':
            ptfields['medium_code'] = "ED"

    elif pt == 'Book' or pt == 'Chapter':
        pass

    elif pt == 'Conference papers' or pt == 'Poster':
        pass

    return ptfields


# -----------------
# Returns fields based on publication type
def get_product_type(pub_type):
    match pub_type:
        case 'Journal article': return 'JA'
        # Note: Many of these have matching codes in v2 (e.g. CO, OT, TR)
        # case 'Monograph' | 'Chapter':
        # See also monographic_title
        #     return 'B'
        # case 'Conference papers' | 'Report' | 'Poster' | 'Internet Publication' | 'Other':
        #     return None

    # If no matches, skip
    return None


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
