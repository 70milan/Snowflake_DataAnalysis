python download_data_gov.py "https://catalog.data.gov/dataset/border-crossing-entry-data-683ae" --only-json
python "c:\Data Engineering\codeprep\Snowflake border crossing\download_data_gov.py" --load-snowflake

=============================================================================================================================

# Download Data.gov Dataset Resources

This folder contains `download_data_gov.py`, a small utility to fetch the dataset
page on Data.gov (or similar pages), discover resource files (CSV/JSON/ZIP/etc.),
and download them to a local folder.

Usage (Windows cmd.exe):

```cmd
python "C:\Data Engineering\codeprep\Snowflake border crossing\download_data_gov.py" "https://catalog.data.gov/dataset/border-crossing-entry-data-683ae" --out downloads
```

Setup:

1. Create and activate your Python environment (optional but recommended).
2. Install dependencies:

```cmd
pip install -r "C:\Data Engineering\codeprep\Snowflake border crossing\requirements.txt"
```

Notes:
- The script will try to parse the HTML for direct file links. If none are found,
  it attempts to query the Data.gov CKAN API to list dataset resources.
- If you want a progress bar, install `tqdm` (included in `requirements.txt`).



