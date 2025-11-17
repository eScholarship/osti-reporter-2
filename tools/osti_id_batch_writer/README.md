# OSTI ID BATCH WRITER

## Purpose
After publications have been submitted to OSTI, their OSTI IDs are saved to the tools database.
This program adds newly-submitted publications to a queuing table, and then adds these pubs'
OSTI IDs to the eScholarship database via the API.

## Program flow

### Enqueue publications
- Sends an SQL query which finds pubs that haven't had their OSTI IDs added to eSchol yet and adds them

### Update pubs loop
- Reads a single row from the queue table in the tools DB
- Sends an access query to the eSchol API to get the pubs' localIDs array
   - Saves this original array for later verification 
- Adds the OSTI ID to the local IDs array
- Verifies the new array contains all the values from the origin array 
- Sends a mutation query to the eSchol API to replace the pubs' localIDs array
- Updates the pub's single row in the queue table, indicating it's been processed.
