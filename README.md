# nets_clean

**Please note - this code was originally meant to replace the nets_wrangle repo, which is convoluted, difficult to follow, and likely missing pieces.  While this code is much cleaner, it is incomplete.**

General Workflow For Creation of the final NETS dataset

Jesse

- Unique locations get created, sent to James.  These correspond to or unique ID, BEH_ID

James

- Geoprocessing performed, sent back to Jesse

Jesse

- Backfilling performed.  This corrects bad geocodes which are often for old addresses with information missing and replaces them with newer, better addresses. No geographic information is needed/included for this other than Loc_name, the level that the business is coded to. James merges in all the rest of the GIS variables later
- Calculate BEH_SIC for each BEH_ID. Assign businesses to non-hierarchy auxiliary categories based on this BEH_SIC. Merge in TDLINX auxiliary categories from Kari.  Create main categories based on these auxiliary categories.
-   Assign businesses to hierarchy auxiliary categories based on established hierarchy, send back to James.

James

- Final data polishing (ie column name formatting), inclusion of GIS variables, joining with other datasets (ie LTDB)

# ToDo

Integrate all Jupyter notebooks into source code:

 *  create_json in its own small script
 *  hierarchy integrated into classify_nets.py
 *  overlap_check and quality_check integrated into a larger testing suit

Alter json config file to use ranges as lists of tuples rather than do it during classification
