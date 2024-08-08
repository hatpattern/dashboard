# dashboard
A telemetry dashboard

## Setup

### VPN

Connection to the Red Hat VPN is required in order to run the script.

### Cookies

You must retrieve 2 cookies to use the script. Follow the instructions and paste in the cookies when prompted.

TODO: Record a video for this

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

On future uses of the script, only steps 2 and 4 are required before or after using the script.

TODO: Write this

TODO: Record a video for this

## Troubleshooting

The most common errors with this script will likely be with the validation of the cookies. If the script fails, try changing the cookie in your script
