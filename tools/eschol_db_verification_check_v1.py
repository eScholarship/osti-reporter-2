import requests
import csv
from dotenv import dotenv_values
from time import sleep
problem_rows = []

config = dotenv_values("../.env")

input_file = csv.DictReader(open("input-2.csv"))
for row in input_file:
    print(f"Requesting ID:{row['id']}, osti_id:{row['osti_id']}")
    sleep(0.25)

    r = requests.get(
        config['OSTI_V1_URL_PROD'],
        auth=(config['OSTI_V1_USERNAME_PROD'],
              config['OSTI_V1_PASSWORD_PROD']),
        params={'osti_id': row['osti_id']})

    if r.status_code != 200:
        print("Non-200 response:")
        print(row)
        problem_rows.append(row)
    elif '<records start="0" rows="0" numfound="0">' in r.text:
        print("Empty record response:")
        print(row)
        problem_rows.append(row)

print("Finished checking loop.")

# Write the problems
if len(problem_rows) == 0:
    print("No OSTI IDs in the check returned problems from the API.")
else:
    print(f"Outputting problems: {len(problem_rows)} total problems")
    keys = problem_rows[0].keys()
    with open('non-200-responses.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(problem_rows)

print("Check completed. Exiting.")
