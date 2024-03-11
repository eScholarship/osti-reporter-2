# Transform for OSTI E-Link Version 1
# OSTI documentation:
# https://www.osti.gov/elink/241-1api.jsp
import json
from copy import deepcopy
import release_info
from pprint import pprint
import xml.etree.ElementTree as ET


# -----------------
# Create the XML bodies for each item in new_osti_v1_pubs
def add_osti_data_v1(new_osti_pubs, test_mode):

    print("Converting SQL results into XML for E-Link v1.")

    new_osti_pubs_with_xml = []
    for pub in new_osti_pubs:

        # ---------------------
        # 1. Create a dict, init with hardcoded items
        osti_pub = deepcopy(release_info.v1)

        # ---------------------
        # 2. Publication-specific
        # Basic pub metadata
        osti_pub['title'] = pub['title']
        osti_pub['doi'] = pub['doi']
        osti_pub['other_identifying_nos'] = pub['ark']
        osti_pub['description'] = pub['abstract']
        osti_pub['publication_date'] = pub['Reporting Date 1']

        # ---------------------
        # 3. XML fields requiring calculation

        # File info
        # Note: DOCX files are converted to PDF in the eScholarship/content/... link.
        if pub['File Extension'] == 'pdf' or pub['File Extension'] == 'docx':
            osti_pub['file_format'] = 'PDFN'
        else:
            print("Publication's file format ≠ pdf or docx, skipping.")
            continue

        # File URL
        # Note: We are using the eschol/content/id/id.pdf URLs here -- these SHOULD always exist.
        if pub['File URL'] is None:
            print("Publication doesn't have an eSchol File URL, skipping.")
            continue
        else:
            osti_pub['site_url'] = pub['File URL']

        # LBL Record numbers
        # This is a required field, will return string "None" if empty.
        osti_pub['report_nos'] = get_lbl_report_number(pub)

        # Authors require JSON-to-text conversion
        osti_pub['author'] = get_v1_authors(json.loads(pub['authors']))

        # Fields relating to the publication type:
        # product_type, medium_code, conference_information, related_doc_info, journal_type
        # These are returned as a dict and merged into osti_pub
        osti_pub.update(get_product_type_fields(pub))

        # Grants are split into an array.
        # These array items are split into individual <sponsor> tags during XML conversion.
        osti_pub['grants'] = [g['name'] for g in json.loads(pub['grants'])]

        # Convert to dict to the OSTI XML format
        osti_pub_xml = dict_to_osti_xml(osti_pub)

        # Convert XML to string formatting
        if test_mode:
            ET.indent(osti_pub_xml)
        osti_pub_xml = ET.tostring(osti_pub_xml, encoding='utf-8', xml_declaration=True)

        pub['submission_xml_string'] = osti_pub_xml

        # Add the item to the list
        new_osti_pubs_with_xml.append(pub)

    return new_osti_pubs_with_xml


# ========================================
# Misc. Helper Functions

def dict_to_osti_xml(pub_dict):
    records = ET.Element('records')
    record = ET.Element('record')

    # OSTI-specific XML tags
    record_status = ET.Element('record_status')
    record_status.append(ET.Element('new'))
    record.append(record_status)

    access_limitation = ET.Element('access_limitation')
    access_limitation.append(ET.Element('unl'))
    record.append(access_limitation)

    # Loop the dict, creating children as needed
    pprint(pub_dict)
    for key in pub_dict.keys():
        if key == 'grants':
            for grant in pub_dict[key]:
                child = ET.Element('sponsor_org')
                child.text = grant
                record.append(child)
        else:
            child = ET.Element(key)
            child.text = str(pub_dict[key])
            record.append(child)

    records.append(record)
    return records


def get_lbl_report_number(pub):
    if pub['LBL Report Number'] is None:
        return "None"
    elif pub['LBL Report Number'][:5] == 'LBNL-':
        return pub['LBL Report Number']
    else:
        return "LBNL-" + pub['LBL Report Number']


def get_v1_authors(authors):
    formatted_authors = []
    for author in authors:
        formatted_author = ""
        if author['last_name'] is not None:
            formatted_author += author['last_name']
        else:
            continue
        if 'first_name' in author.keys():
            formatted_author += (", " + author['first_name'])
        if 'middle_name' in author.keys():
            formatted_author += (" " + author['middle_name'])
        formatted_authors.append(formatted_author)

    return "; ".join(formatted_authors)


def get_product_type_fields(pub):
    pt = {}

    # If the pub has an LBL report number, override its type with 'report'
    if pub['LBL Report Number'] is not None:
        pub['Type'] = 'report'

    match_pub_type = pub['Type'].lower()

    if match_pub_type == 'journal article':
        # Journal, OA, and medium (electronic document)
        pt['product_type'] = 'JA'
        pt['journal_type'] = 'AM'
        pt['medium_code'] = 'ED'

        # Journal, Volume, issue (if available)
        if pub['Journal Name'] is not None:
            pt['journal_name'] = pub['Journal Name']

        if pub['volume'] is not None:
            pt['journal_volume'] = pub['volume']

        if pub['issue'] is not None:
            pt['journal_issue'] = pub['issue']

        if pub['volume'] is not None:
            pt['journal_volume'] = pub['volume']

        if pub['issue'] is not None:
            pt['journal_issue'] = pub['issue']

    elif match_pub_type == 'monograph' or match_pub_type == 'chapter':
        pt['product_type'] = 'B'
        pt['medium_code'] = 'ED'
        if pub['Type'].lower() == 'chapter' and pub['parent-title'] is not None:
            pt['related_doc_info'] = "Book Title: " + pub['parent-title']

    elif match_pub_type == 'conference papers' or match_pub_type == 'poster':
        pt['product_type'] = 'CO'
        pt['medium_code'] = 'ED'
        if pub['name-of-conference'] is not None:
            pt['conference_information'] = pub['name-of-conference']

    elif match_pub_type == 'report':
        pt['product_type'] = 'TR'
        pt['medium_code'] = 'ED'

    else:
        pt['product_type'] = 'UNKNOWN'

    return pt
