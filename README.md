# OSTI Reporter program
LBL is required to submit DOE-funded papers to OSTI, which we do via OSTI's E-Link API. This a rewrite of the former Ruby program, co-inciding with OSTI moving to E-Link version 2.

## Program Workflow
1. Query MySQL osti db for a list of pubs we've already submitted
2. Elements:
   1. Create a temp table from the MySQL data
   2. Query to get the new publications we haven't sent to OSTI yet
3. Translate the Elements pubs into OSTI-formatted XML (v1) or JSON (v2)
4. Submit the XMLs (v1) or JSONS (v2) to the OSTI API
   1. Add the OSTI responses and OSTI IDs to the data 
5. Update the MySQL osti db with new (successful) submission data (incl. the new OSTI IDs)

## QA / Testing options
* This program can produce output for E-Link v1 (XML), and E-Link v2 (JSON). These can be selected with the argument -v 1 or -v 2.
* QA args:
  * -x / --test : Output .xml or .json files to disk, skipping the submission and mysql db updates. 
  * -iq / --input-qa : Pull data from Elements QA reporting db
  * -eq / --elink-qa : Send to OSTI's staging databases
  * -oq / --output-qa : Update the staging mysql db
* A typical QA run ```python3 run_osti_reporter -v 1 -eq -oq``` will take prod data as input, and proceed to use QA for the ELink submissions and mysql db updates.

## Subi Specifics
Subi is running Python 3.7, so there's a few things to be aware of:
* Ext. package "requests" 2.26.0 needed (current requests version uses urllib3 which has deprecated SSL connections <1.0.2, which are used in py 3.7)
* Python's ElementTree tostring() had some significant changes with string formatting from 3.7 to the current version (exporting in byte string vs str, [see here](https://stackoverflow.com/questions/33814607/converting-a-python-xml-elementtree-to-a-string)) -- some workarounds are patched in, with current python methods in the comments. 