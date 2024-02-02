import json
import datetime
import xml.etree.ElementTree as ET
from pprint import pprint


# ========================================
# Transform for OSTI E-Link Version 1

# -----------------
# Create the XML bodies for each item in new_osti_v1_pubs
def add_osti_data_v1(new_osti_pubs, test_mode):

    print("Converting SQL results into XML for E-Link v1.")

    osti_pubs_xml = []

    for pub in new_osti_pubs:

        # ---------------------
        # 1. Create a dict, init with hardcoded items
        osti_pub = {

            # Misc.
            'language': "English",
            'country_publication_code': "US",

            # LBL details
            'site_input_code': "LBNLSCH",
            'doe_contract_nos': "AC02-05CH11231",
            'originating_research_org':
                "Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)",

            # Release info
            'released_by': "Geoff Hamm",
            'released_date': (datetime.datetime.now()).strftime('%m/%d/%Y'),
            'released_by_email': "ghamm@lbl.gov",
            'released_by_phone': "510-495-2633"

        }

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
        if pub['File Extension'] == 'pdf':
            osti_pub['file_format'] = 'PDFN'
        elif pub['File Extension'] == 'docx':
            osti_pub['file_format'] = 'DOCX'
        else:
            print("Publication's file format ≠ pdf or docx, skipping.")
            continue

        # File URL
        # Note: We are using the eschol/content/id/id.pdf URLs here.
        if pub['File URL'] is None:
            print("Publication doesn't have an eSchol File URL, skipping.")
            continue
        else:
            osti_pub['site_url'] = pub['File URL']

        # LBL Record numbers
        report_no = get_lbl_report_number(pub)
        if report_no is not False:
            osti_pub['report_nos'] = report_no

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

        # XML to string formatting
        if test_mode:
            ET.indent(osti_pub_xml)
        osti_pub_xml = ET.tostring(osti_pub_xml, encoding='ascii', xml_declaration=True)

        # Add the item to the list
        osti_pubs_xml.append(osti_pub_xml)

    return osti_pubs_xml


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
        return False
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

    match pub['Type'].lower():

        case 'journal article':
            pt['product_type'] = 'JA'

            # Journal, Volume, issue (if available)
            if pub['journal'] is not None:
                pt['journal_name'] = pub['journal']
            if pub['volume'] is not None:
                pt['journal_volume'] = pub['volume']
            if pub['issue'] is not None:
                pt['journal_issue'] = pub['issue']

            # Filetype and OA
            pt['journal_type'] = 'AM'
            pt['medium_code'] = 'ED'

            # These medium types are for open access URLs, which we no longer use
            # else:
            #    pt['journal_type'] = 'AC'
            #    pt['medium_code'] = 'X'

        case 'monograph' | 'chapter':
            pt['product_type'] = 'B'
            pt['medium_code'] = 'ED'
            if pub['Type'].lower() == 'chapter' and pub['parent-title'] is not None:
                pt['related_doc_info'] = "Book Title: " + pub['parent-title']

        case 'conference papers' | 'poster':
            pt['product_type'] = 'CO'
            pt['medium_code'] = 'ER'
            if pub['name-of-conference'] is not None:
                pt['conference_information'] = pub['name-of-conference']

        case 'report':
            pt['product_type'] = 'TR'
            pt['medium_code'] = 'ED'

        case _:
            pt['product_type'] = 'UNKNOWN'

    return pt
