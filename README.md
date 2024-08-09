# dashboard
A telemetry dashboard

## Setup

### VPN

Connection to the Red Hat VPN is required in order to run the script.

### Cookies

You must retrieve 2 cookies to use the script.

One cookie comes from amplitude which you can get in the devtools/inspect element pannel under network whenver you export a csv. This cookie goes in line one of the `cookies.txt` file. The second cookie you retrieve from the redhat subscription service.

### Service account

This script requires a service account that the main spreadsheet is shared with. Follow the instructions in the documentation of the gspread library for this.

### Virtual environment

While it's possible to install all of the dependencies to your system, it's a good practice to use a virtual environment to set this up, do the following commands:

1. Set up the virtual environment
  - `python3 -m venv venv`

2. Activate the virtual environment
  - `source venv/bin/activate`

3. Install the dependencies
  - `pip3 install -r deps`

4. After running the script, deactivate the virtual environment like so
  - `deactivate`


## Useage

Once you have activated the virtual environment, simply run `python finaldash.py`

## Troubleshooting

The most common errors with this script will likely be with the validation of the cookies. If the script fails, try changing the cookie in your script

Further, there are occasional transient errors where it will fail to scrape an orgid. This is okay, simply re-run the script.
