import pandas as pd
import numpy as np

import concurrent.futures
import requests
from io import StringIO
from bs4 import BeautifulSoup

from datetime import datetime
from datetime import date

import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

## Step 1 Authenticate to google sheets
gc = gspread.service_account('./service-account.json')
dashboard = gc.open('Lightspeed Telemetry Dashboard')

if not dashboard:
    raise Exception("The dashboard could not be loaded")

## Step 2 Get cookies

# TODO: Add argparse cookie collection
with open('./cookies.txt', 'r') as cookiefile:
    amplitude_cookie = cookiefile.readline().rstrip()
    redhat_cookie = cookiefile.readline().rstrip()

## Step 3 get amplitude
amplitude_headers = {
    'Host': 'analytics.amplitude.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://app.amplitude.com/',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://app.amplitude.com',
    'Connection': 'keep-alive',
    "Cookie": amplitude_cookie.encode("utf-8")}
amplitude_request_data="definition=%7B%22colorAssignments%22%3A%7B%7D%2C%22customSerieLabels%22%3A%7B%7D%2C%22customXAxisLabels%22%3A%7B%7D%2C%22disabledParamPaths%22%3A%5B%5D%2C%22featureTags%22%3A%7B%7D%2C%22filter%22%3A%22%22%2C%22hasLoaded%22%3Atrue%2C%22id%22%3Anull%2C%22name%22%3A%22Completions+per+Org+IDs+with+modelNames+and+Users+%28with+org+filter%29%22%2C%22params%22%3A%7B%22interval%22%3A1%2C%22segments%22%3A%5B%7B%22name%22%3A%22%E2%89%A0+Lightspeed+Internal%2C+Community+Test%2C+Test+Cohort%22%2C%22conditions%22%3A%5B%7B%22op%22%3A%22is+not%22%2C%22prop%22%3A%22userdata_cohort%22%2C%22type%22%3A%22property%22%2C%22values%22%3A%5B%22x195odl%22%2C%227ycpahuz%22%2C%22z6igeqt%22%5D%2C%22prop_type%22%3A%22user%22%2C%22group_type%22%3A%22User%22%7D%5D%2C%22_id%22%3A%22b2da2b79-6513-4635-ba93-d683e10f65e4%22%2C%22label%22%3A%22%22%7D%5D%2C%22groupBy%22%3A%5B%5D%2C%22rows%22%3A%5B%7B%22id%22%3A%22overall%22%2C%22label%22%3A%22Overall%22%2C%22params%22%3A%7B%22filter%22%3A%5B%5D%2C%22group_by%22%3A%5B%7B%22type%22%3A%22event%22%2C%22value%22%3A%22rh_user_org_id%22%2C%22group_type%22%3A%22User%22%7D%2C%7B%22type%22%3A%22event%22%2C%22value%22%3A%22modelName%22%2C%22group_type%22%3A%22User%22%7D%2C%7B%22type%22%3A%22user%22%2C%22value%22%3A%22user_id%22%2C%22group_type%22%3A%22User%22%7D%5D%7D%2C%22parentIdPath%22%3A%5B%5D%7D%5D%2C%22range%22%3A%22Last+90+Days%22%2C%22table%22%3A%7B%22rows%22%3A%5B%7B%22dimensionHierarchy%22%3A%7B%22subDimensions%22%3A%5B%5D%2C%22dimensionsWithFilters%22%3A%5B%7B%22filters%22%3A%5B%5D%2C%22dimension%22%3A%7B%22type%22%3A%22GROUP_BY%22%2C%22params%22%3A%7B%22groupBy%22%3A%7B%22type%22%3A%22event%22%2C%22value%22%3A%22rh_user_org_id%22%2C%22group_type%22%3A%22User%22%7D%7D%7D%7D%2C%7B%22filters%22%3A%5B%5D%2C%22dimension%22%3A%7B%22type%22%3A%22GROUP_BY%22%2C%22params%22%3A%7B%22groupBy%22%3A%7B%22type%22%3A%22event%22%2C%22value%22%3A%22modelName%22%2C%22group_type%22%3A%22User%22%7D%7D%7D%7D%2C%7B%22filters%22%3A%5B%5D%2C%22dimension%22%3A%7B%22type%22%3A%22GROUP_BY%22%2C%22params%22%3A%7B%22groupBy%22%3A%7B%22type%22%3A%22user%22%2C%22value%22%3A%22user_id%22%2C%22group_type%22%3A%22User%22%7D%7D%7D%7D%5D%2C%22parentDimensionFilters%22%3A%5B%5D%7D%7D%5D%2C%22columns%22%3A%5B%7B%22id%22%3A%22adab7bf8-7068-48b9-8b2c-e4befc0a8909%22%2C%22type%22%3A%22METRIC%22%2C%22params%22%3A%7B%22metric%22%3A%7B%22id%22%3A%22%22%2C%22name%22%3A%22%22%2C%22type%22%3A%22TOTALS%22%2C%22appId%22%3A%22428233%22%2C%22orgId%22%3A%22170477%22%2C%22params%22%3A%7B%22type%22%3A%22TOTALS%22%2C%22params%22%3A%7B%22event%22%3A%7B%22_id%22%3A%222e53acb4-a40d-4945-a957-d719ee435e33%22%2C%22filters%22%3A%5B%5D%2C%22group_by%22%3A%5B%5D%2C%22event_type%22%3A%22completion%22%7D%2C%22countGroup%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22attributionParams%22%3Anull%2C%22multiPropertyFilters%22%3A%5B%5D%7D%7D%2C%22deleted%22%3Afalse%2C%22version%22%3A1%2C%22createdAt%22%3A0%2C%22createdBy%22%3A%22%22%2C%22deletedAt%22%3Anull%2C%22deletedBy%22%3Anull%2C%22updatedAt%22%3Anull%2C%22updatedBy%22%3Anull%2C%22isOfficial%22%3Afalse%2C%22description%22%3A%22%22%7D%2C%22filters%22%3A%5B%7B%22group_type%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22subprop_op%22%3A%22is%22%2C%22subprop_key%22%3A%22rh_user_has_seat%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22True%22%5D%7D%2C%7B%22group_type%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22rh_user_org_id%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22%28none%29%22%2C%221979710%22%2C%228070623%22%2C%2211688131%22%2C%2212508446%22%2C%2213441753%22%2C%2213859376%22%2C%2216037076%22%2C%2217329057%22%2C%2217380863%22%2C%2217443992%22%2C%2217557146%22%2C%2213162754%22%2C%2217233726%22%2C%2212444807%22%2C%226528611%22%2C%2217218092%22%2C%2211015602%22%2C%2212521983%22%2C%2213147084%22%2C%2211009103%22%2C%2213247185%22%2C%22852656%22%2C%226340056%22%2C%2215746564%22%2C%229999999999%22%2C%2217321086%22%2C%2213141780%22%2C%2217507144%22%2C%2211331435%22%2C%224340048%22%2C%2217691404%22%2C%2214077179%22%2C%2211789772%22%2C%2214090544%22%2C%2217261415%22%2C%2211231301%22%2C%2215782727%22%2C%2213038026%22%2C%228082919%22%2C%2215467510%22%2C%2213015479%22%2C%2212638394%22%2C%2212878962%22%2C%2213771296%22%2C%2216481794%22%2C%2217695330%22%2C%2217694947%22%2C%2217698734%22%2C%2217699360%22%2C%2216081073%22%2C%2216217840%22%2C%2216187451%22%2C%2212695877%22%2C%2213738920%22%2C%2215884643%22%2C%227708734%22%2C%2212310465%22%2C%227271256%22%2C%2213722165%22%2C%2217782365%22%2C%2217777981%22%2C%2215843917%22%2C%2213091065%22%2C%2215910785%22%2C%2217797812%22%2C%2215959250%22%2C%2217803656%22%2C%2212849719%22%2C%227870487%22%2C%2215274723%22%2C%2217663081%22%2C%2217815107%22%2C%2217810914%22%2C%2213075222%22%2C%2217781009%22%5D%7D%2C%7B%22group_type%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22modelName%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22%28none%29%22%2C%22language%3Dansible%22%2C%22ansible-wisdom-v11%22%5D%7D%5D%2C%22segments%22%3A%5B%5D%7D%2C%22dimensionHierarchy%22%3A%7B%22subDimensions%22%3A%5B%5D%2C%22dimensionsWithFilters%22%3A%5B%5D%2C%22parentDimensionFilters%22%3A%5B%5D%7D%7D%5D%2C%22sorting%22%3A%7B%22period%22%3Anull%2C%22columnId%22%3A%22adab7bf8-7068-48b9-8b2c-e4befc0a8909%22%2C%22direction%22%3A%22desc%22%2C%22columnType%22%3A%22metric%22%2C%22segmentIndex%22%3A0%7D%7D%2C%22columns%22%3A%5B%7B%22id%22%3A%22adab7bf8-7068-48b9-8b2c-e4befc0a8909%22%2C%22type%22%3A%22METRIC%22%2C%22params%22%3A%7B%22metric%22%3A%7B%22id%22%3A%22%22%2C%22name%22%3A%22%22%2C%22type%22%3A%22TOTALS%22%2C%22appId%22%3A%22428233%22%2C%22orgId%22%3A%22170477%22%2C%22params%22%3A%7B%22type%22%3A%22TOTALS%22%2C%22params%22%3A%7B%22type%22%3A%22TOTALS%22%2C%22event%22%3A%7B%22_id%22%3A%222e53acb4-a40d-4945-a957-d719ee435e33%22%2C%22filters%22%3A%5B%5D%2C%22group_by%22%3A%5B%5D%2C%22event_type%22%3A%22completion%22%7D%7D%7D%2C%22deleted%22%3Afalse%2C%22version%22%3A1%2C%22createdAt%22%3A0%2C%22createdBy%22%3A%22%22%2C%22deletedAt%22%3Anull%2C%22deletedBy%22%3Anull%2C%22updatedAt%22%3Anull%2C%22updatedBy%22%3Anull%2C%22isOfficial%22%3Afalse%2C%22description%22%3A%22%22%2C%22isRestricted%22%3Afalse%7D%2C%22filters%22%3A%5B%7B%22group_type%22%3A%22User%22%2C%22subfilters%22%3A%5B%5D%2C%22subprop_op%22%3A%22is%22%2C%22subprop_key%22%3A%22rh_user_has_seat%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22True%22%5D%7D%2C%7B%22group_type%22%3A%22User%22%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22rh_user_org_id%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22%28none%29%22%2C%221979710%22%2C%228070623%22%2C%2211688131%22%2C%2212508446%22%2C%2213441753%22%2C%2213859376%22%2C%2216037076%22%2C%2217329057%22%2C%2217380863%22%2C%2217443992%22%2C%2217557146%22%2C%2213162754%22%2C%2217233726%22%2C%2212444807%22%2C%226528611%22%2C%2217218092%22%2C%2211015602%22%2C%2212521983%22%2C%2213147084%22%2C%2211009103%22%2C%2213247185%22%2C%22852656%22%2C%226340056%22%2C%2215746564%22%2C%229999999999%22%2C%2217321086%22%2C%2213141780%22%2C%2217507144%22%2C%2211331435%22%2C%224340048%22%2C%2217691404%22%2C%2214077179%22%2C%2211789772%22%2C%2214090544%22%2C%2217261415%22%2C%2211231301%22%2C%2215782727%22%2C%2213038026%22%2C%228082919%22%2C%2215467510%22%2C%2213015479%22%2C%2212638394%22%2C%2212878962%22%2C%2213771296%22%2C%2216481794%22%2C%2217695330%22%2C%2217694947%22%2C%2217698734%22%2C%2217699360%22%2C%2216081073%22%2C%2216217840%22%2C%2216187451%22%2C%2212695877%22%2C%2213738920%22%2C%2215884643%22%2C%227708734%22%2C%2212310465%22%2C%227271256%22%2C%2213722165%22%2C%2217782365%22%2C%2217777981%22%2C%2215843917%22%2C%2213091065%22%2C%2215910785%22%2C%2217797812%22%2C%2215959250%22%2C%2217803656%22%2C%2212849719%22%2C%227870487%22%2C%2215274723%22%2C%2217663081%22%2C%2217815107%22%2C%2217810914%22%2C%2213075222%22%2C%2217781009%22%5D%7D%2C%7B%22group_type%22%3A%22User%22%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22modelName%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22%28none%29%22%2C%22language%3Dansible%22%2C%22ansible-wisdom-v11%22%5D%7D%5D%2C%22segments%22%3A%5B%5D%7D%7D%5D%2C%22countGroup%22%3A%22User%22%7D%2C%22recycledColors%22%3A%5B%5D%2C%22selectedSet%22%3A%7B%7D%2C%22selectingMaxAllowed%22%3Afalse%2C%22seriesSize%22%3A0%2C%22sortedBy%22%3A%7B%22columnIndex%22%3Anull%2C%22columnId%22%3Anull%2C%22dimensionIndex%22%3Anull%2C%22direction%22%3Anull%7D%2C%22userSelected%22%3Afalse%2C%22viewParams%22%3A%7B%22columnConfigs%22%3A%7B%22ag-Grid-AutoColumn%22%3A%7B%22width%22%3A303%7D%2C%22group-by-col-tasks%22%3A%7B%22width%22%3A173%7D%2C%22group-by-col-user_id%22%3A%7B%22width%22%3A213%7D%2C%22empty-global-group-by%22%3A%7B%22width%22%3A193%7D%2C%22group-by-col-modelName%22%3A%7B%22width%22%3A670%7D%2C%22group-by-col-promptType%22%3A%7B%22width%22%3A209%7D%2C%22group-by-col-rh_user_org_id%22%3A%7B%22width%22%3A228%7D%2C%22group-by-col-rh_user_has_seat%22%3A%7B%22width%22%3A241%7D%2C%22adab7bf8-7068-48b9-8b2c-e4befc0a8909%22%3A%7B%22width%22%3A248%2C%22sortDirection%22%3A%22desc%22%7D%7D%7D%2C%22customizedTopSelectionValue%22%3Anull%2C%22app%22%3A%22428233%22%2C%22type%22%3A%22dataTableV2%22%2C%22version%22%3A37%2C%22description%22%3A%22%22%2C%22shouldShowConciseDateFormat%22%3Atrue%2C%22shouldShowTwoYearRange%22%3Afalse%2C%22currencyLocale%22%3A%22en-us%22%7D&downloadId=&chart_id=vqxplqr9&edit_id=xbeqj1ti&headers=%5B%5D"
completions_csv_request = requests.post("https://analytics.amplitude.com/data/428233/csv", headers=amplitude_headers, allow_redirects=True, verify=False, data=amplitude_request_data)
amplitude_daterange_request_data="definition=%7B%22vis%22%3A%22line%22%2C%22recycledColors%22%3A%5B%5D%2C%22app%22%3A%22428233%22%2C%22colorAssignments%22%3A%7B%7D%2C%22shouldShowTwoYearRange%22%3Afalse%2C%22params%22%3A%7B%22segments%22%3A%5B%7B%22name%22%3A%22All+Users%22%2C%22conditions%22%3A%5B%5D%2C%22_id%22%3A%22b2da2b79-6513-4635-ba93-d683e10f65e4%22%2C%22label%22%3A%22%22%7D%5D%2C%22interval%22%3A1%2C%22nthTimeLookbackWindow%22%3A365%2C%22formula%22%3A%22UNIQUES%28A%29%22%2C%22metric%22%3A%22formula%22%2C%22countGroup%22%3A%22User%22%2C%22groupBy%22%3A%5B%5D%2C%22events%22%3A%5B%7B%22_id%22%3A%221c4a19a2-b247-4fab-b93c-bee6dbf8dd44%22%2C%22filters%22%3A%5B%5D%2C%22group_by%22%3A%5B%7B%22type%22%3A%22event%22%2C%22value%22%3A%22rh_user_org_id%22%2C%22group_type%22%3A%22User%22%7D%5D%2C%22event_type%22%3A%22completion%22%7D%5D%2C%22range%22%3A%22Last+90+Days%22%7D%2C%22userSelected%22%3Afalse%2C%22customXAxisLabels%22%3A%7B%7D%2C%22selectedSet%22%3A%7B%7D%2C%22customSerieLabels%22%3A%7B%7D%2C%22selectingMaxAllowed%22%3Afalse%2C%22viewParams%22%3A%7B%22metrics%22%3A%7B%22takeawayType%22%3A%22OVERALL_VALUE%22%7D%7D%2C%22currencyLocale%22%3A%22en-us%22%2C%22name%22%3A%22Completions+by+OrgID+with+past+90+days+of+activity%22%2C%22featureTags%22%3A%7B%7D%2C%22shouldShowConciseDateFormat%22%3Atrue%2C%22sortedBy%22%3A%7B%22columnIndex%22%3Anull%2C%22columnId%22%3Anull%2C%22dimensionIndex%22%3Anull%2C%22direction%22%3Anull%7D%2C%22version%22%3A37%2C%22seriesSize%22%3A100%2C%22filter%22%3A%22%22%2C%22customizedTopSelectionValue%22%3Anull%2C%22type%22%3A%22eventsSegmentation%22%2C%22id%22%3Anull%2C%22description%22%3A%22%22%2C%22disabledParamPaths%22%3A%5B%5B%22histogramConfigBin%22%5D%2C%5B%22customBuckets%22%5D%5D%2C%22hasLoaded%22%3Atrue%7D&downloadId=q3U9VUC&chart_id=undefined&edit_id=undefined&headers=%5B%5B%22https%3A%2F%2Fapp.amplitude.com%2Fanalytics%2Fredhat%2Fchart%2Fnew%2Ftovo0sh7%22%5D%5D"
#amplitude_daterange_request_data="definition=%7B%22vis%22%3A%22line%22%2C%22recycledColors%22%3A%5B%5D%2C%22app%22%3A%22428233%22%2C%22colorAssignments%22%3A%7B%7D%2C%22shouldShowTwoYearRange%22%3Afalse%2C%22params%22%3A%7B%22interval%22%3A1%2C%22segments%22%3A%5B%7B%22name%22%3A%22%E2%89%A0+Lightspeed+Internal%2C+Community+Test%2C+Test+Cohort%22%2C%22conditions%22%3A%5B%7B%22op%22%3A%22is+not%22%2C%22prop%22%3A%22userdata_cohort%22%2C%22type%22%3A%22property%22%2C%22values%22%3A%5B%22x195odl%22%2C%227ycpahuz%22%2C%22z6igeqt%22%5D%2C%22prop_type%22%3A%22user%22%2C%22group_type%22%3A%22User%22%7D%5D%2C%22_id%22%3A%22b2da2b79-6513-4635-ba93-d683e10f65e4%22%2C%22label%22%3A%22%22%7D%5D%2C%22groupBy%22%3A%5B%5D%2C%22range%22%3A%22Last+90+Days%22%2C%22events%22%3A%5B%7B%22_id%22%3A%22b4778cfa-87ef-46d4-b756-714aaf3dbb10%22%2C%22filters%22%3A%5B%7B%22group_type%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22subprop_op%22%3A%22is%22%2C%22subprop_key%22%3A%22rh_user_has_seat%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22True%22%5D%7D%2C%7B%22group_type%22%3A%7B%22name%22%3A%22User%22%2C%22is_computed%22%3Afalse%7D%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22modelName%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22%28none%29%22%2C%22language%3Dansible%22%2C%22ansible-wisdom-v11%22%5D%7D%2C%7B%22group_type%22%3A%22User%22%2C%22subprop_op%22%3A%22is%22%2C%22subprop_key%22%3A%22response.status_code%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%22200%22%5D%7D%2C%7B%22group_type%22%3A%22User%22%2C%22subprop_op%22%3A%22is+not%22%2C%22subprop_key%22%3A%22rh_user_org_id%22%2C%22subprop_type%22%3A%22event%22%2C%22subprop_value%22%3A%5B%229999999999%22%5D%7D%5D%2C%22group_by%22%3A%5B%7B%22type%22%3A%22event%22%2C%22value%22%3A%22rh_user_org_id%22%2C%22group_type%22%3A%22User%22%7D%5D%2C%22event_type%22%3A%22completion%22%7D%5D%2C%22metric%22%3A%22totals%22%2C%22countGroup%22%3A%22User%22%2C%22nthTimeLookbackWindow%22%3A365%7D%2C%22userSelected%22%3Afalse%2C%22customXAxisLabels%22%3A%7B%7D%2C%22selectedSet%22%3A%7B%7D%2C%22customSerieLabels%22%3A%7B%7D%2C%22selectingMaxAllowed%22%3Afalse%2C%22viewParams%22%3A%7B%22metrics%22%3A%7B%22takeawayType%22%3A%22OVERALL_VALUE%22%7D%7D%2C%22currencyLocale%22%3A%22en-us%22%2C%22name%22%3A%22Completions+by+OrgID+with+past+90+days+of+activity%22%2C%22featureTags%22%3A%7B%7D%2C%22shouldShowConciseDateFormat%22%3Atrue%2C%22sortedBy%22%3A%7B%22columnIndex%22%3Anull%2C%22columnId%22%3Anull%2C%22dimensionIndex%22%3Anull%2C%22direction%22%3Anull%7D%2C%22version%22%3A37%2C%22seriesSize%22%3A100%2C%22filter%22%3A%22%22%2C%22customizedTopSelectionValue%22%3Anull%2C%22type%22%3A%22eventsSegmentation%22%2C%22id%22%3Anull%2C%22description%22%3A%22%22%2C%22disabledParamPaths%22%3A%5B%5D%2C%22hasLoaded%22%3Atrue%7D&downloadId=qchqrTe&chart_id=undefined&edit_id=undefined&headers=%5B%5B%22https%3A%2F%2Fapp.amplitude.com%2Fanalytics%2Fredhat%2Fchart%2F5tv4x94y%22%5D%5D"

daterange_csv_request = requests.post("https://analytics.amplitude.com/data/428233/csv", headers=amplitude_headers, allow_redirects=True, verify=False, data=amplitude_daterange_request_data)

if completions_csv_request.status_code != 200:
    print(completions_csv_request)
    raise Exception("Request for completions csv failed")

if daterange_csv_request.status_code != 200:
    print(completions_csv_request)
    raise Exception("Request for daterange csv failed")

completions = StringIO(completions_csv_request.text)
daterange = StringIO(daterange_csv_request.text)

## Step 4 get completions into pandas
completions_df = pd.read_csv(completions, skiprows=3)
completions_df.columns = ['orgid', 'modelname', 'userid', 'completions']

grouped = completions_df.groupby('orgid')

# Create smaller DataFrames for each OrgID
orgid_data = []
for orgid, smaller_df in grouped:
    # Create a dictionary with the 'orgid', the count of entries, the sum of 'completions', and the number of unique 'modelname'
    entry = {
        'orgid': orgid, 
        'models': smaller_df['modelname'].nunique(),
        'users': len(smaller_df), 
        'completions': smaller_df['completions'].sum()
    }
      
    # Add the dictionary to the list
    orgid_data.append(entry)


# Convert the list of dictionaries to a DataFrame and sort the DataFrame by the 'completions' column in descending order
df = pd.DataFrame(orgid_data)
df = df.sort_values('completions', ascending=False)

# print("DATA")
# print(df.to_string())


## Step 5 Scrape the data from redhat

def get_org_page(orgid, cookie):
    headers = {"Cookie": cookie.encode("utf-8")}
    response = requests.get(f"https://subscription-admin.corp.redhat.com/user/show?id={orgid}", headers=headers, allow_redirects=True, verify=False)
    return response.content

def get_user_page(orgid, cookie):
    headers = {"Cookie": cookie.encode("utf-8")}
    response = requests.get(f"https://subscription-admin.corp.redhat.com/user/find?search_string={orgid}&commit=Search&lookup_type=org_id", headers=headers, allow_redirects=True, verify=False)
    return response.content

def get_subscription_page(orgid, cookie):
    headers = {"Cookie": cookie.encode("utf-8")}
    response = requests.get(f"https://subscription-admin.corp.redhat.com/user/{orgid}/subscriptions", headers=headers, allow_redirects=True, verify=False)
    return response.content

# POOR MAN'S SINGLETON
_sku_sheet = None
_sku_column = None
def get_lightspeed_skus():
    # Please forgive me, for I have sinned
    global _sku_sheet
    global _sku_column
    if _sku_column:
        #print("REUSING")
        return [cell[0] for cell in _sku_column]
    else:
        #print("GETTING FROM API")
        sku_sheet = dashboard.worksheet('Lightspeed-SKUs')
        _sku_sheet = sku_sheet
        skus_column = sku_sheet.get("A2:A")
        _sku_column = skus_column
        return [cell[0] for cell in _sku_column]


def scrape_org(orgid, cookie):
    # Make the request
    orgpage = get_org_page(orgid, cookie)
    # Parse it
    soup = BeautifulSoup(orgpage, 'html.parser')
    # Find the org name
    company_div = soup.find(lambda tag: tag.name == 'div' and "Company Name:" in tag.text, class_="emphasized")
    org_name = company_div.text.split(": ")[1].strip()
    # Find the account number
    account_num_div = soup.find(lambda tag: tag.name == 'div' and "Account Number:" in tag.text, class_="emphasized")
    account_number = account_num_div.text.split(": ")[1].strip()

    userpage = get_user_page(orgid, cookie)
    soup = BeautifulSoup(userpage, 'html.parser')
    # Find the user count (max 200)
    user_a_tags = soup.find_all('a', href=lambda x: "/user/show" in x)
    user_count = len(user_a_tags)
    # Find the first email
    first_email_td = soup.find(lambda tag: tag.name == 'td' and "@" in tag.text)
    first_email = first_email_td.text

    # Get the subscription
    subscriptionpage = get_subscription_page(orgid, cookie)
    soup = BeautifulSoup(subscriptionpage, 'html.parser')
    # Get Expired subs
    expired = soup.find('table', id='expired_subs_table')
    expired_subs = expired.find_all('a', href=lambda x: "offerings" in x)
    get_date_node = lambda node: node.parent.find_next_sibling().find_next_sibling().find_next_sibling()
    expired_subs = sorted(expired_subs, key=lambda x: datetime.strptime(get_date_node(x).get_text(), '%m/%d/%Y'))
    expired_subs = expired_subs[::-1] #using this idiom and not reversed() because I don't think you can iterate more than once? it was throwing an error for me when I tried
    # Get the active subs
    active = soup.find('table', id='active_subs_table')
    active_subs = active.find_all('a', href=lambda x: "offerings" in x)
    lightspeed_skus = get_lightspeed_skus()
    # If there's an active sub, we want to return that
    for sku in lightspeed_skus:
        for active_sub in active_subs:
            if sku in active_sub.get_text():
                return {'account': account_number,
                        'orgname': org_name,
                        'accounts': user_count,
                        'email': first_email,
                        'active_sku': sku,
                        'expired_sku': None,
                        'expiration_date': None}
    # If there's not, we'll find the most recently expired lightspeed-giving sku
    capable_subs = []
    for sku in lightspeed_skus:
        for expired_sub in expired_subs:
            date = get_date_node(expired_sub).get_text()
            if sku in expired_sub.get_text():
                return {'account': account_number,
                        'orgname': org_name,
                        'accounts': user_count,
                        'email': first_email,
                        'active_sku': None,
                        'expired_sku': expired_sub.get_text(),
                        'expiration_date': date}
    return {
        'account': account_number,
        'orgname': org_name,
        'accounts': user_count,
        'email': first_email,
        'active_sku': "Other",
        'expired_sku': None,
        'expiration_date': None}

orgid_list = df['orgid'].unique()

orgid_datadict = {}
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Submit scraping tasks to the executor
    future_to_orgid = {executor.submit(scrape_org, orgid, redhat_cookie): orgid for orgid in orgid_list}
    
    # Retrieve results as they complete
    for future in concurrent.futures.as_completed(future_to_orgid):
        orgid = future_to_orgid[future]
        try:
            scraped_data = future.result()  # Get the result of the completed task
            orgid_datadict[orgid] = scraped_data
            #print(f"scraped {orgid}")
        except Exception as e:
            print(f"Error scraping orgid {orgid}: {e}")

# Pair the scraped data to the dataframe
df[['account','orgname','accounts','email','active_sku', 'expired_sku', 'expiration_date']] = df['orgid'].apply(lambda id: pd.Series(orgid_datadict[id]))

print("SCRAPED")
#print(df.to_string())
print(df['orgid'].sort_values().to_string())

## Step 6: Process daterange data

dates_df = pd.read_csv(daterange, skiprows=6)

# Initialize an empty DataFrame to hold the first column and the result from idxmax
dateprocess_df = pd.DataFrame(columns=['orgid', 'new_date_start', 'new_date_end'])

for index, row in dates_df.iloc[:, 1:].iterrows():
    # Find the first non-zero value and get the column label
    first_non_zero_column = (row != 0).idxmax()

    # Find the last non-zero value and get the column label
    last_non_zero_column = row.replace(0, pd.NA).last_valid_index()
    orgid = dates_df.iloc[index, 0]

    # Create a new DataFrame for this row
    row_df = pd.DataFrame({'orgid': [orgid], 'new_date_start': [first_non_zero_column], 'new_date_end': [last_non_zero_column]})
    
    # Concatenate the new DataFrame with the existing DataFrame
    dateprocess_df = pd.concat([dateprocess_df, row_df], ignore_index=True)

dateprocess_df = dateprocess_df.dropna(subset=['orgid'])
print("Dates:")
#print(dateprocess_df.to_string())
print(dateprocess_df['orgid'].sort_values().to_string())
## Step 7: Read in the history of the dates

# Get dataframe froms heets
running_final_sheet = dashboard.worksheet("running-final")
running_final = get_as_dataframe(running_final_sheet)


# Standardize the types
dateprocess_df['orgid'] = pd.to_numeric(dateprocess_df['orgid'])
dateprocess_df[['new_date_start', 'new_date_end']] = dateprocess_df[['new_date_start', 'new_date_end']].apply(lambda x: pd.to_datetime(x, format='mixed'))
running_final['orgid'] = pd.to_numeric(running_final['orgid'])
running_final[['start_date', 'end_date']] = running_final[['start_date', 'end_date']].apply(pd.to_datetime)

# Find the proper start and end dates
merged_dates = pd.merge(dateprocess_df, running_final, on='orgid', how='outer')

def find_oldest_start_date(row):
    if pd.notnull(row['new_date_start']):
        return min(row['new_date_start'], row['start_date'])
    else:
        return row['start_date']

def find_newest_end_date(row):
    if pd.notnull(row['new_date_end']):
        return max(row['new_date_end'], row['end_date'])
    else:
        return row['end_date']

merged_dates['oldest_start_date'] = merged_dates.apply(find_oldest_start_date, axis=1)
merged_dates['newest_end_date'] = merged_dates.apply(find_newest_end_date, axis=1)
final_dates = merged_dates[['orgid', 'oldest_start_date', 'newest_end_date']]

final_data = pd.merge(df, final_dates, on='orgid')
final_data['email'] = final_data['email'].str.split('@').str[1]

## Step 8
# Separate customer and internal users
# TODO: Use the orgid list in the spreadsheet to create this instead
dashboard.worksheet('Organizations-dynamic-cohorts')
final_customer_data = final_data[~final_data['email'].str.contains('ibm.com|redhat.com')]

new_customers_df = final_customer_data[~final_customer_data.orgid.isin(running_final.orgid)]
append_df = running_final[~running_final.orgid.isin(final_customer_data.orgid)]

set_with_dataframe(running_final_sheet, append_df)

## Step 9: Ouput the results into google sheets
today = date.today()
today_str = today.strftime('%B %d, %Y')

# Get the dashboard worksheet
def get_dashboard_worksheet(daystr):
    # Copy the template
    template  = dashboard.worksheet("dashboard-template")
    worksheet_title = f"{daystr} Dashboard"
    worksheets_list = dashboard.worksheets()
    # First make sure the worksheet already exists
    for worksheet in worksheets_list:
        if worksheet.title == worksheet_title:
            return worksheet
    # If it doesn't then we can create it
    template.duplicate(insert_sheet_index=0, new_sheet_name=worksheet_title)
    return worksheet

dashboard_mainsheet = get_dashboard_worksheet(today_str)
header = f"As of {today_str}"
subtitle = "(over the past 90 days)"
blank = ""
# TODO: Fix me (totals or 90 days be pacific)
total_orgs = str(final_customer_data['orgid'].nunique())
total_users = str(final_customer_data['users'].sum())
total_completions = str(final_customer_data['completions'].sum())

#print(top_five_rows)
top_five_rows = final_customer_data.nlargest(5, 'completions')
top_five_rows = top_five_rows[['orgname', 'completions', 'users']]
top_five_leaderboard = top_five_rows.values.tolist()

sorted_by_start_date = append_df.sort_values(by=['oldest_start_date', 'completions']).reset_index(drop=True)

def standardize_times(month, year):
    return month + year*12
month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'} 
month = 0
year = 0
date_totals = {}
# Get the total labels
for row in sorted_by_start_date[['oldest_start_date', 'orgname']].itertuples():
    stdized = standardize_times(row[1].month, row[1].year)
    if stdized in date_totals:
        date_totals[stdized] += 1
    else:
        date_totals[stdized] = 1

monthly_adoptions_rows = []
for row in sorted_by_start_date[['oldest_start_date', 'orgname']].itertuples():
    row_date = row[1]
    row_name = row[2]
    row_month = row_date.month
    row_year = row_date.year
    date_label = ""
    total_label = ""
    if standardize_times(month, year) < standardize_times(row_month, row_year):
        month = row_month
        year = row_year
        date_label = f"{month_names[row_month]} {row_year}"
        total_label = date_totals[standardize_times(row_month, row_year)]
    monthly_adoptions_rows.append([date_label, total_label, row_name])

updates = [
    {'range': 'A1:A2',
     'values': [[header], [subtitle]]},
    {'range': 'B4:C6',
     'values': [["Total number of organizations", total_orgs],
                ["Total number of users", total_users],
                ["Total number of completions", total_completions]]},
    {'range': 'A8:A8',
     'values': [["The Top 5 Organizations by Completions (over the last 90 days)"]]},
    {'range': 'B9:D9',
     'values': [["Org", "Completions", "Users"]]},
    {'range': 'B10:D14', 
     'values': top_five_leaderboard},
    {'range': 'F4:F4',
     'values': [["Monthly Customer Activations (LS+WCA)"]]},
    {'range': 'F5:H',
     'values': monthly_adoptions_rows}]

dashboard_mainsheet.batch_update(updates)

