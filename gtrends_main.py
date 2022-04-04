# Authenticate Google account
from oauth2client.service_account import ServiceAccountCredentials 
import gspread
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/content/drive/MyDrive/pytrend/pytrend project key.json', scope)
gc = gspread.authorize(creds)

# variable to indicate the starting time
startTime = time.time()


# Import Libraries and Packages.
from pytrends.request import TrendReq
import pandas as pd
import time
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe


# Open Google sheet and get the keywords and country from it.
wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xjSF0sJ3ZKbuorLkyHvJG69cwTV2BDq_RJ-R88czufo/edit#gid=0')

keywords_sheet = wb.worksheet("gtrends_keywords")
listHQ = keywords_sheet.col_values(1)
listHQ.remove("keywords")

geoHQ = keywords_sheet.acell('B4').value

# timeframe for HQ
tfHQ = keywords_sheet.acell('B2').value

listC = keywords_sheet.col_values(3)
listC.remove("keywords")

# timeframe for all countries
tfC = keywords_sheet.acell('F2').value

listUK = keywords_sheet.col_values(5)
listUK.remove("keywords")

geoAT = keywords_sheet.acell('D2').value
geoBE = keywords_sheet.acell('D3').value
geoDK = keywords_sheet.acell('D4').value
geoFR = keywords_sheet.acell('D5').value
geoDE = keywords_sheet.acell('D6').value
geoNL = keywords_sheet.acell('D7').value
geoIE = keywords_sheet.acell('D8').value
geoES = keywords_sheet.acell('D9').value
geoSE = keywords_sheet.acell('D10').value
geoCH = keywords_sheet.acell('D11').value
geoGB = keywords_sheet.acell('D12').value
geoUS = keywords_sheet.acell('D13').value


# Create a function to get the Google Trends data and store the results in a variable, query the list, and get the result
pytrend = TrendReq(hl='en-GB', tz=360, timeout=(10,25))

def gtrend (keywordlist,country,tf):
    '''The function takes the necessary argument and return Google Trends data in DataFrame'''
    dataset = []
    for x in range(0,len(keywordlist)):
        keywords = [keywordlist[x]]
        pytrend.build_payload(kw_list = keywords, cat=0, timeframe = tf, geo = country)
        data = pytrend.interest_over_time()
        if not data.empty:
            data = data.drop(labels=['isPartial'],axis='columns')
            dataset.append(data)

    result = pd.concat(dataset, axis=1)
    result = result.reset_index()
    result['date'] = pd.to_datetime(result['date']).dt.date

    return result

# Export in 3 new sheets (Max 30 columns) inside the same Google sheet we opened above.
output1 = wb.worksheet("Gtrends1")
output2 = wb.worksheet("Gtrends2")
output3 = wb.worksheet("Gtrends3")


exportHQ = set_with_dataframe(output1, pd.DataFrame(gtrend(listHQ,geoHQ,tfHQ)),col=1,row=2)
exportAT = set_with_dataframe(output1, pd.DataFrame(gtrend(listC,geoAT,tfC)),col=4,row=2)
exportBE = set_with_dataframe(output1, pd.DataFrame(gtrend(listC,geoBE,tfC)),col=9,row=2)
exportDK = set_with_dataframe(output1, pd.DataFrame(gtrend(listC,geoDK,tfC)),col=14,row=2)
exportFR = set_with_dataframe(output1, pd.DataFrame(gtrend(listC,geoFR,tfC)),col=19,row=2)
exportDE = set_with_dataframe(output1, pd.DataFrame(gtrend(listC,geoDE,tfC)),col=24,row=2)
exportNL = set_with_dataframe(output2, pd.DataFrame(gtrend(listC,geoNL,tfC)),col=1,row=2)
exportIE = set_with_dataframe(output2, pd.DataFrame(gtrend(listC,geoIE,tfC)),col=6,row=2)
exportES = set_with_dataframe(output2, pd.DataFrame(gtrend(listC,geoES,tfC)),col=11,row=2)
exportSE = set_with_dataframe(output2, pd.DataFrame(gtrend(listC,geoSE,tfC)),col=16,row=2)
exportCH = set_with_dataframe(output2, pd.DataFrame(gtrend(listC,geoCH,tfC)),col=21,row=2)
exportGB = set_with_dataframe(output2, pd.DataFrame(gtrend(listUK,geoGB,tfC)),col=26,row=2)
exportUS = set_with_dataframe(output3, pd.DataFrame(gtrend(listC,geoUS,tfC)),col=1,row=2)


# Print Excecution time
executionTime = (time.time() - startTime)
print('Execution time in sec.: ' + str(executionTime))