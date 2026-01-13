# OSTI ID BATCH WRITER

## Purpose
After publications have been submitted to OSTI, their OSTI IDs are saved to the pub-oapi-tools-rds DB.
This program reads from that table, updating the eSchol item's local_IDs with the OSTI ID, via the eSchol API.

This is a batch-processing program, but this code will eventually be integrated into the OSTI reporter as
the final step after the publication submissions to OSTI's E-Link API.

## Program flow
- Grabs the appropriate connections based on the 'testing_mode' flag
- Loops the following:
- Reads a single row from the queue table in the tools DB
- Sends an access query to the eSchol API to get the pubs' localIDs array
   - Saves this original array for later verification 
- Adds the OSTI ID to the local IDs array
- Verifies the new array contains all the values from the origin array 
- Sends a mutation query to the eSchol API to replace the pubs' localIDs array
- Updates the pub's single row in the queue table, indicating it's been processed.
