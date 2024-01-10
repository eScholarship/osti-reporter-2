# OSTI Reporter program
LBL is required to submit DOE-funded papers to OSTI, which we do via OSTI's E-Link API. This a rewrite of the former Ruby program, co-inciding with OSTI moving to E-Link version 2.

This program can produce output for E-Link v1 (XML), and E-Link v2 (JSON). These can be selected with the argument -v 1 or -v 2.

Args also provide options for connecting to QA, Production, and tunneling. Run with -h for help.