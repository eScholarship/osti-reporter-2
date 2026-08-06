import batch_release_info
from copy import deepcopy
from datetime import datetime


def prep_row_for_transform(row, group):
    if group == 'NAWI':
        return transform_nawi(row)
    elif group == 'QSA':
        return transform_qsa(row)
    else:
        raise "The group specified in prep_for_transform() " \
              "isn't recognized. Please check the global vars " \
              "at the top of batch_osti_upload.py"


# ===================================
def transform_qsa(row):
    def get_product_type_and_subfields(pub):

        return_dict = {'product_type': 'JA',
                       'journal_type': 'AM',
                       'journal_name': pub['Journal']}
        return return_dict

        journal_types = ["Article",
                         "Featured Article",
                         "Journal",
                         "Preview",
                         "research Article (NAWI mentions)",
                         'Technical Brief (NAWI Mentions)',
                         "Special Issue"]

        if pub['Publication Type'] in journal_types:
            return_dict = {'product_type': 'JA',
                           'journal_type': 'AM',
                           'journal_name': pub['Journal']}
            if pub['Volume']:
                return_dict['volume'] = pub['Volume']
            if pub['Issue']:
                return_dict['issue'] = pub['Issue']
            return return_dict

        elif pub['Publication Type'] in ['Conference Presentation',
                                         'Conference Presentation']:
            return {
                'product_type': 'CO',
                'conference_type': 'A',
                'conference_information': pub['Journal']}

        elif pub['Publication Type'] == 'Book Chapter':
            return {'product_type': 'B'}

        else:
            print(f"Empty type??: {pub['ID#']}, {pub['Publication Type']}")
            return_dict = {'product_type': 'JA',
                           'journal_type': 'AM',
                           'journal_name': pub['Journal']}
            if pub['Volume']:
                return_dict['volume'] = pub['Volume']
            if pub['Issue']:
                return_dict['issue'] = pub['Issue']

            return return_dict

    # ------------------------
    # MAIN transform

    osti_row = deepcopy(batch_release_info.v2)
    osti_row.update(get_product_type_and_subfields(row))

    row_key_swaps = {'Title': 'title',
                     'date': 'publication_date',
                     'DOI': 'doi'}

    for csv_key, elements_key in row_key_swaps.items():
        osti_row[elements_key] = row.get(csv_key)

    # sanitize date
    if osti_row['publication_date']:
        try:
            date_obj = datetime.strptime(osti_row['publication_date'], "%Y-%m-%d").date()
            osti_row['publication_date'] = date_obj.strftime("%Y-%m-%d")
        except:
            osti_row['publication_date'] = f"{row['Year']}-01-01"
    else:
        osti_row['publication_date'] = f"{row['Year']}-01-01"

    # sanitize authors
    author_persons = []
    for author in row['authors'].split(', '):
        author_dict = dict(type="AUTHOR")
        name_parts = author.split(' ')
        author_dict['last_name'] = name_parts.pop(-1)
        if name_parts:
            author_dict['first_name'] = name_parts.pop(0)
        if name_parts:
            author_dict['middle_name'] = " ".join(name_parts)
        author_persons.append(author_dict)
    osti_row['persons'].extend(author_persons)

    return osti_row


# ===================================
def transform_nawi(row):
    def get_product_type_and_subfields(pub):
        journal_types = ["Article",
                         "Featured Article",
                         "Journal",
                         "Preview",
                         "research Article (NAWI mentions)",
                         'Technical Brief (NAWI Mentions)',
                         "Special Issue"]

        if pub['Publication Type'] in journal_types:
            return_dict = {'product_type': 'JA',
                           'journal_type': 'AM',
                           'journal_name': pub['Journal']}
            if pub['Volume']:
                return_dict['volume'] = pub['Volume']
            if pub['Issue']:
                return_dict['issue'] = pub['Issue']
            return return_dict

        elif pub['Publication Type'] in ['Conference Presentation',
                                         'Conference Presentation']:
            return {
                'product_type': 'CO',
                'conference_type': 'A',
                'conference_information': pub['Journal']}

        elif pub['Publication Type'] == 'Book Chapter':
            return {'product_type': 'B'}

        else:
            print(f"Empty type??: {pub['ID#']}, {pub['Publication Type']}")
            return_dict = {'product_type': 'JA',
                           'journal_type': 'AM',
                           'journal_name': pub['Journal']}
            if pub['Volume']:
                return_dict['volume'] = pub['Volume']
            if pub['Issue']:
                return_dict['issue'] = pub['Issue']

            return return_dict

    # ------------------------
    # MAIN transform

    osti_row = deepcopy(batch_release_info.v2)
    osti_row.update(get_product_type_and_subfields(row))

    row_key_swaps = {'Title': 'title',
                     'Date Published': 'publication_date',
                     'URL': 'doi'}

    for csv_key, elements_key in row_key_swaps.items():
        osti_row[elements_key] = row.get(csv_key)

    # sanitize date
    if osti_row['publication_date']:
        try:
            date_obj = datetime.strptime(osti_row['publication_date'], "%m/%d/%y").date()
            osti_row['publication_date'] = date_obj.strftime("%Y-%m-%d")
        except:
            osti_row['publication_date'] = f"{row['Year']}-01-01"
    else:
        osti_row['publication_date'] = f"{row['Year']}-01-01"

    persons = []
    for author in row['Authors'].split(', '):
        author_dict = dict(type="AUTHOR")
        name_parts = author.split(' ')
        author_dict['last_name'] = name_parts.pop(-1)
        if name_parts:
            author_dict['first_name'] = name_parts.pop(0)
        if name_parts:
            author_dict['middle_name'] = " ".join(name_parts)

        persons.append(author_dict)

    osti_row['persons'].extend(persons)

    return osti_row