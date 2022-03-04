"""
turnrow logistics sharing tool

author: efedorko@downstreamstrategies.com, https://github.com/erghjunk/
date: february 2022

intended use is as an ESRI geoprocessing REST service.

accepts inputs of a full delivery address, a number of cases or palletsto be delivered,
and a deliver by date. determines all possible ways within the week of delivery that these
products can be delivered w/in multi-partner logistics network; also checks truck capacity (generally) and
whether the delivery address is w/in 5 miles of a route.

returns JSON w/a header that includes a flag describing two possible return states - fail state w/a
message and success state with 1+n messages.

v 0.3 uses csv files and PANDAS to read them which is... stupid
v 0.4 shifts all source data over to SQLite, though still uses the same bad data model
v 0.5 abandoned attempt at various things
v 0.6 adds several things to the final output and formats it all as json; json used in web app dev testing
v 0.6 (deploy) collapses everything into a single directory and adds pre-run dirctory cleanup, input pre-processing, and AddMessage output
v 0.6 (as deployed) has several changes made in order to make this script operate as an ESRI GP service; was deployed as an ArcGIS service,
which meant using python 2.7 instead of 3.x; this neccessitated getting rid of f strings for .format()

TO RUN LOCALLY, see instructions on line 58
"""

from arcpy import MakeFeatureLayer_management, SelectLayerByLocation_management, MakeXYEventLayer_management, SearchCursor, GetCount_management, env, AddMessage
import sys
import datetime
import os
from geopy.geocoders import Nominatim
import shutil
import sqlite3 as sq
import json

env.overwriteOutput = True

# service inputs

# delivByDate will need parsed and converted into datetime.date; original test input was datetime.date(2021, 6, 18)
# wrote a convertor here on assumption that value is a string of "mm/dd/yyyy" from an html <input type="date"> element
# dateString = arcpy.GetParameterAsText(0)
# dateSplit = dateString.split("/")
# delivByDate = datetime.date(int(dateSplit[2]), int(dateSplit[0]), int(dateSplit[1]))
# destA = arcpy.GetParameterAsText(1)  # address expects string
# destC = arcpy.GetParameterAsText(2)  # city expects string
# destS = arcpy.GetParameterAsText(3)  # state expects string
# destZ = arcpy.GetParameterAsText(4)  # zip expects string
# quantity = int(arcpy.GetParameterAsText(5))  # expects integer; convert for safety
# unit = arcpy.GetParameterAsText(6)  # cases or pallets, expects string


# local testing inputs
# to use, comment out service inputs (above), uncomment one address block (below) as well as
# delivByDate, quantity (~lines 110 & 111), and one instance of unit (~ line 113 or 114)
# also change "ws" var on line 124 to current directory
# uncomment print statement at the end to print outputJson to the console to verify results

destA = "123 Pleasant Street"
destC = "Morgantown"
destS = "West Virginia"
destZ = "26505"

# destA = "505 Lee Street"
# destC = "Lewisburg"
# destS = "West Virginia"
# destZ = "24901"

# destA = "312 Ohio Avenue"
# destC = "Charleston"
# destS = "West Virginia"
# destZ = "25302"

# destA = "1704 N Eastman Rd"
# destC = "Kingsport"
# destS = "Tennessee"
# destZ = "37664"

# destA = "117 S Main St"
# destC = "Blacksburg"
# destS = "Virginia"
# destZ = "24060"

# this one will fail (outside of area)
# destA = "123 E Main Street"
# destC = "Columbus"
# destS = "Ohio"
# destZ = "43215"

delivByDate = datetime.date(2022, 6, 18)
quantity = 2
# unit can be cases or pallets
unit = "cases"
# unit = "pallets"

# end of local test inputs

# processing inputs
# destFull is the required format for the Nominatim geocoder; needs to stay in code even if geocoder (and format) changes because it's used elsewhere
# geocoding takes place in findRegion()
destFull = destA + ", " + destC + ", " + destS + ", " + destZ
ws = os.getcwd()
ingeodb = ws + r"\logistics.gdb"
logTo = ws + r"\logfile.txt"
stopFlag = 0
nextRegionFlag = 0


# this is too complicated and repetitive and will require another script just to populate tables everytime we add a partner or region
# some future version can fix this because it's just as complicatd to change as it is to use this
# THE SELF-TAUGHT PROGRAMMER'S DILEMMA
# SQL database use in this script is read only
# database setup
"""
Table Names:
ASD_RegionDay   PartnerData     RegionCodes     Routes
GG_RegionDay    PartnerTruck2   RegionDays      SF_RegionDay
GOV_RegionDay   RA_RegionDay    RegionRouteDay
"""
sourcedb = ws + r"\logistics.db"
dbc = sq.connect(sourcedb)
cur = dbc.cursor()

# processig outputs
region = ""
regions = []
partnerName = ""
partnerPOC = ""
partnerPhone = ""
partnerEmail = ""
partnerAddress = ""
capacityFlag = 0
capacityMessage = ""
routeIndex = 0
destX = ''
destY = ''

# quit message holds an early exit information and is also a potential final output stored in "message"
quitMessage = "No delivery available."
allRoutes = {}


# tools

# pre-run clean up; previous failed runs may leave text files behind and this deletes them
def preRunCleanup():
    if os.path.exists(logTo):
        os.remove(logTo)
    if os.path.exists(ws + r"\destination.txt"):
        os.remove(ws + r"\destination.txt")


# logging file function. log file is saved under new name at the end in main program.
def writeToLog(news):
    logfile = open(logTo, 'a')
    logfile.write(news)
    logfile.write('\n')
    logfile.close()
    return


# puts single quotes around a string for use in an SQL query
def quoter(text):
    return "'" + text + "'"


# _S_ingle _V_alue query
def sv_query(target_value, table, clause_target, clause):
    # query = f"SELECT {target_value} FROM {table} WHERE {clause_target} = " + quoter(clause)
    query = "SELECT {} FROM {} WHERE {} = ".format(target_value, table, clause_target)
    query += quoter(clause)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        value = row[0]
    return value


# ensures that input date is at least one week in the future
# uses stopFlag which can end main program early
def checkDates():
    today = datetime.date.today()
    currentWeek = today.isocalendar()[1]
    global delivByDate
    deliveryWeek = delivByDate.isocalendar()[1]
    if today < delivByDate and currentWeek < deliveryWeek:
        writeToLog("Today is before due date.")
    else:
        writeToLog("Quitting.")
        global quitMessage
        quitMessage = "Due date must be at least one week in the future."
        global stopFlag
        stopFlag = 1
    return


# seems bizarre that we need this but it would appear that this is a requirement for MakeXYEventLayer;
# the fn is called ONCE, but file is used TWICE, first in findRegion(), then in checkIfNear()
def XYtextFile(content, name):
    target = open(ws + "\\" + name, 'a')
    target.write("ID, X, Y")
    target.write('\n')
    target.write(content)
    target.close()


# expects a string in the format "Day #" for dayToReturn
# this version deals with the "previous saturday or sunday problem"
def returnDate(startDate, startDateNum, dayToReturn):
    # e.g. "Day 2"
    dayStrings = dayToReturn.split(" ")
    # e.g. "2"
    returnDayNum = int(dayStrings[1])
    # e.g. 5-2 = 3
    if returnDayNum < startDateNum:
        dayDelta = startDateNum - returnDayNum
        # e.g. 3 days before startDate
        dateOut = startDate - datetime.timedelta(days=dayDelta)
        return dateOut.strftime('%a %m-%d-%Y')
    else:
        # use this to replace the offset in the dayDelta calculation
        # replacement for Day 0 is at tuple index position 0, etc;
        convert = (7, 6, 5, 4, 3, 2, 1)
        dayDelta = abs(startDateNum - convert[returnDayNum])
        dateOut = startDate - datetime.timedelta(days=dayDelta)
        return dateOut.strftime('%a %m-%d-%Y')


# geocode desination and identify region
# ensure that destination is within region
# make list of region(s) for further processing
# uses stopFlag which can end main program early
def findRegion():
    writeToLog("Getting regions.")
    locator = Nominatim(user_agent="http://www.turnrowfarms.org/")
    location = locator.geocode(destFull)
    destXY = "1, " + str(location.longitude) + ", " + str(location.latitude)
    global destX, destY
    destX = str(location.longitude)
    destY = str(location.latitude)
    writeToLog("Destination is " + destXY)
    XYtextFile(destXY, "destination.txt")
    MakeXYEventLayer_management(ws + "\\" + "destination.txt", "X", "Y", "destLyr")
    MakeFeatureLayer_management(ingeodb + "\\Turnrow_Regions_WGS84_2", "regionsLyr")
    SelectLayerByLocation_management("regionsLyr", "intersect", "destLyr", "", "NEW_SELECTION")
    clause = ""
    rows = SearchCursor("regionsLyr", clause)
    test = 0
    for row in rows:
        test += 1
    if test > 0:
        rows = SearchCursor("regionsLyr", clause)
        for row in rows:
            global regions
            regions.append(row.TurnrowRegion)
            writeToLog("Region found: " + str(row.TurnrowRegion))
    else:
        writeToLog("Quitting.")
        global quitMessage
        quitMessage = "Delivery location is outside of all delivery areas."
        global stopFlag
        stopFlag = 1


# compare quantity and unit to the already identified partner's truck capacity and return yes/no as 1/0
def truckCheck(partner, quant, unitt):
    evaluate = sv_query(unitt, "PartnerTruck2", "PartnerCode", partner)
    if int(evaluate) >= quant:
        truckOK = 1
    else:
        truckOK = 0
    return truckOK


# checks if delivery destination is within 5 miles of delivery route being evaluated
# and returns appropriate message
def checkIfNear(route):
    fullRoute = ingeodb + "\\" + route
    MakeFeatureLayer_management(fullRoute, "routeLyr")
    MakeXYEventLayer_management(ws + "\\" + "destination.txt", "X", "Y", "destLyr2")
    SelectLayerByLocation_management("routeLyr", "WITHIN_A_DISTANCE", "destLyr2", "5 Miles", "NEW_SELECTION")
    result = GetCount_management("routeLyr")
    matchcount = int(result[0])
    if matchcount == 0:
        message = "This destination is greater than 5 miles from delivery route and is likely to incur extra charges."
    else:
        message = "This destination is within 5 miles of delivery route and can be delivered for normal rates."
    return message


# identify candidate delivery days in the same week of delivery due date
def getOptions(destRegion, dueDate):
    regionCode = sv_query("Code", "RegionCodes", "Region", destRegion)
    regionMonths = sv_query("Months", "RegionCodes", "Region", destRegion).split(",")
    delivMonthNum = int(dueDate.strftime("%m"))
    # seasonality check; checks that deliveries take place in that region during the month of the delivery due date
    # this ends current iteration of getOptions and moves to the next region in the list (if there are multiple)
    # by way of returning to the current loop in the main program (for region in regions) quitMessage will
    # propagate forward and be returned only if all regions fail to have any delivery options
    if str(delivMonthNum) not in regionMonths:
        global quitMessage
        quitMessage = "No delivery available. There is no delivery in this region during the requested month."
        writeToLog(quitMessage)
        return
    delivDayNum = dueDate.weekday()
    # these next few lines create a list of numbers to lookup to see if there is delivery that day
    testDelivDays = [delivDayNum]
    x = delivDayNum - 1
    while x >= 0:
        testDelivDays.append(x)
        x -= 1
    for day in testDelivDays:
        writeToLog("working on day " + str(day))
        route = sv_query(regionCode, "RegionRouteDay", "DayNum", str(day))  # returns a 0 (string) or a route code
        writeToLog("our route code is: " + route)
        if route == '0':
            pass
        else:
            partnerCode = sv_query("Owner", "Routes", "Code", route)
            writeToLog("Partner Code: " + partnerCode)
            global capacityFlag
            capacityFlag = truckCheck(partnerCode, quantity, unit)
            writeToLog("Capacity Flag: " + str(capacityFlag))
            if capacityFlag == 0:
                global capacityMessage
                capacityMessage = "Shipping is available, but this shipment is likely too large for delivery truck to handle. Please contact carrier to verify."
            # this section returns the aggregation date and appends to final output
            tableName = str(partnerCode) + "_RegionDay"
            writeToLog("searching table: " + tableName)
            delivByValue = sv_query(regionCode, tableName, "DayNum", str(day))
            writeToLog("we got a deliver by value of: " + delivByValue)
            dateOfDelivery = returnDate(dueDate, delivDayNum, "Day " + str(day))
            dateToAggregate = returnDate(dueDate, delivDayNum, delivByValue)
            routeFC = sv_query("FC", "Routes", "Code", route)
            # check proximity of destination to route
            distanceMessage = checkIfNear(routeFC)
            partnerName = sv_query("Name", "PartnerData", "PartnerCode", partnerCode)
            candidateRoute = {
                "destFull": destFull,
                "routeCode": route,
                "routeFC": routeFC,
                "partnerCode": partnerCode,
                "partnerName": partnerName,
                "aggDate": dateToAggregate,
                "delivDate": dateOfDelivery,
                "distanceMessage": distanceMessage,
                "capacity": capacityMessage,
                "partnerPOC": sv_query("POC", "PartnerData", "PartnerCode", partnerCode),
                "partnerPhone": sv_query("Phone", "PartnerData", "PartnerCode", partnerCode),
                "partnerEmail": sv_query("Email", "PartnerData", "PartnerCode", partnerCode),
                "partnerAddress": sv_query("Address", "PartnerData", "PartnerCode", partnerCode)
            }
            global routeIndex
            global allRoutes
            allRoutes[routeIndex] = candidateRoute
            routeIndex += 1


# Main Program
preRunCleanup()
checkDates()
writeToLog("Dates checked. Finding regions.")
findRegion()
# stopFlag will flip if:
# destination is outside of region geographies
# date problem: date not in future or region not serviced that month
# outcome is stored in var quitMessage
if stopFlag == 0:
    writeToLog("starting getOptions loop")
    for region in regions:
        writeToLog("getOptions is working on " + str(region) + " and " + str(delivByDate))
        getOptions(region, delivByDate)
    if len(allRoutes) == 0:
        outputBlob = {"outputFlag": 0, "message": quitMessage}  # outputBlob is FINAL OUTPUT
        writeToLog(quitMessage)  # quitMessage is a FINAL OUTPUT
    else:
        writeToLog("Writing logistics now.")
        # changing routeIndex here will impact how the JS loop iterates in the web app so don't do it
        outputBlob = {"outputFlag": 1, "destX": destX, "destY": destY, "routes": routeIndex - 1, "message": allRoutes}
        writeToLog(str(outputBlob))  # outputBlob is FINAL OUTPUT
else:
    outputBlob = {"outputFlag": 0, "destX": destX, "destY": destY, "message": quitMessage}  # outputBlob is FINAL OUTPUT
    writeToLog(quitMessage)
os.remove(ws + "\\" + "destination.txt")
timeString = str(datetime.datetime.now().time()).replace(":", "").replace(".", "")
shutil.copy(logTo, ws + r"\logs\log_" + timeString + ".txt")
os.remove(logTo)
outputJson = json.dumps(outputBlob, indent=5)
print(outputJson)
AddMessage(outputJson)
# arcpy.SetParameterAsText(7, outputJson)
