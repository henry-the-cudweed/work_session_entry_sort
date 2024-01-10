#CREATE PIVOT_WORK_SESSION_CREATED.XLSX
#then generate organized excel sheet

#planning_sheet is from a csv i have with some data on individual patches

#work_session_entry.csv is created by copy/pasting the CF work session entry lookup page
#into an excel sheet and converting to CSV
# expected column headers: ,Project,Date,Activity,Crew,Role,# People,Person-Hours,Reference,Notes,

#calflora-out.csv exported from group observations

#region imports
import os
import pandas as pd
import time
import datetime
import re
import geopandas as gpd
import shapely


#endregion

#region delete old files
# Specify the file names for the existing files
pivot_work_session_file = 'pivot_work_session.xlsx'
# Check if the files exist and delete them if they do
if os.path.exists(pivot_work_session_file):
    os.remove(pivot_work_session_file)
    print(f"Previous file {pivot_work_session_file} has been deleted.")
#endregion
    
#region load work_session_entry.csv and clean up
# Assuming you have the WorkSessionEntry DataFrame
work_session_entry = pd.read_csv('work_session_entry.csv') 

work_session_entry['Reference'] = work_session_entry['Reference'].str.lower()

# Convert 'Person-Hours' column in work_session_entry to numeric in case it's not already
work_session_entry['Person-Hours'] = pd.to_numeric(work_session_entry['Person-Hours'], errors='coerce')
#endregion

#region create pivot_work_session with columns for each work date, add total hours column
# Pivot the WorkSessionEntry DataFrame with proper aggregation using aggfunc='sum'
pivot_work_session = work_session_entry.pivot_table(index='Reference', columns='Date', values='Person-Hours', aggfunc='sum')

# Reset the index to make 'Reference' a regular column
pivot_work_session = pivot_work_session.reset_index()

# Add 'Total Hours' column to the pivot_work_session DataFrame
pivot_work_session['Total Hours'] = pivot_work_session.iloc[:, 1:-1].sum(axis=1).astype(str)

# Reorganize columns to have 'Total Hours' immediately to the right of 'Reference'
column_order = ['Reference', 'Total Hours'] + list(pivot_work_session.columns[1:-1])
pivot_work_session = pivot_work_session[column_order]

# Export the pivot_work_session DataFrame to an Excel file in the same folder
pivot_work_session.to_excel(pivot_work_session_file, index=False)

print(f"Data has been exported to {pivot_work_session_file}")
#endregion

#region Load the pivot_work_session_created.xlsx file
pivot_work_session_file = 'pivot_work_session.xlsx'
pivot_work_session = pd.read_excel(pivot_work_session_file)
#endregion

#region load planning_sheet
planning_sheet = pd.read_csv('planning_sheet.csv')  
#endregion

#region create reference_columns, date_columns
# Extract columns from the PlanningSheet DataFrame based on the 'Reference' column
reference_columns = ['Canyon','Status', 'Next return','state of patch', 'est person-hours remaining']

# Get the date columns from pivot_work_session
date_columns = [col for col in pivot_work_session.columns if pivot_work_session[col].dtype == 'datetime64[ns]']
#endregion

#region Create sort_date function
# return 1 brings oldest-newest
# return -1 brings newest-oldest
def sort_date(value):
    try:
        return 1 * time.mktime(datetime.datetime.strptime(value, "%m/%d/%Y").timetuple())
    except:
        return float("-inf")

#endregion

#region column organization    
# Define the desired column order
other_columns = list(pivot_work_session.columns.difference(date_columns + ['Reference', 'Total Hours'] + reference_columns))

# Sort other_columns into organized date_columns
date_columns = sorted(other_columns, key=sort_date)

column_order = ['Reference', 'Total Hours'] + reference_columns + date_columns

# Reorganize columns
merged_data = pd.merge(pivot_work_session, planning_sheet[['Reference'] + reference_columns], on='Reference', how='left')
merged_data = merged_data[column_order]

#endregion column organization 

#region load calflora-out.csv and create df calflora_data out of a subset of columns
calflora_out = pd.read_csv('calflora-out.csv')  

# Select the desired columns
calflora_columns = ["ID",'Gross Area', 'Common Name', 'Percent Cover',"Latitude","Longitude"]
calflora_data = calflora_out[calflora_columns]
#endregion

#region Merge calflora_data with merged_data using the correct columns
merged_data = pd.merge(merged_data, calflora_data, left_on='Reference', right_on='ID', how='outer')
#endregion

#region combine Reference and ID columns
# Create a new column 'Merged_ID_Reference' combining 'Reference' and 'ID'
merged_data['Merged_ID_Reference'] = merged_data['Reference'].combine_first(merged_data['ID'])

# Drop reduntant columns and rename merged_id_reference to reference
merged_data.drop('ID', axis=1, inplace=True)
merged_data.drop('Reference', axis=1, inplace=True)
merged_data['Reference'] = merged_data['Merged_ID_Reference']
#merged_data.drop(['Merged_ID_Reference'])

#endregion

#region add Calflora links
url_prefix = 'https://www.calflora.org/entry/poe.html#vrid='
merged_data['Link'] = url_prefix + merged_data['Reference']
#endregion

#region create 'Est Infested Cover Range' column 
#Extract numerical values from "Percent Cover" column and convert to numeric
merged_data[['Low End', 'High End']] = merged_data['Percent Cover'].str.extractall('(\d+)').astype(float).unstack()

# Fill missing values in "High End" with values from "Low End"
merged_data['High End'].fillna(merged_data['Low End'], inplace=True)

# Extract numeric part from 'Infested Area' and convert to numeric
merged_data['Gross Area'] = merged_data['Gross Area'].str.extract('([\d.]+)').astype(float)

# Convert 'Infested Area', 'Low End', and 'High End' columns to numeric
merged_data['Low End'] = pd.to_numeric(merged_data['Low End'], errors='coerce')
merged_data['High End'] = pd.to_numeric(merged_data['High End'], errors='coerce')

# Replace NaN values with 0 in 'Infested Area', 'Low End', and 'High End' columns
#merged_data[['Infested Area', 'Low End', 'High End']] = merged_data[['Infested Area', 'Low End', 'High End']].fillna(0)

# Create new columns "low est gross cover" and "high est gross cover"
merged_data['low est gross cover'] = merged_data['Gross Area'] * merged_data['Low End'] / 100
merged_data['high est gross cover'] = merged_data['Gross Area'] * merged_data['High End'] / 100


# Create a new column "est gross cover range"
merged_data['Est Infested Cover Range'] = merged_data.apply(lambda row: 
    "Missing Area Value" if pd.isna(row['Gross Area']) 
    else f"{row['low est gross cover']} - {row['high est gross cover']} sq m" 
        if row['low est gross cover'] != row['high est gross cover'] 
        else f"{row['low est gross cover']} sq m", axis=1
)
#endregion

#region reorganize merged_data columns
merged_data = merged_data[['Canyon','Common Name','Link','Reference', 'Total Hours','Next return',  'est person-hours remaining','Status', 'state of patch','Gross Area',  'Percent Cover', 'Est Infested Cover Range'] + date_columns + ["Latitude","Longitude"]]
#endregion

#region import geojson_string polygons for canyons
geojson_string = """
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "Canyon": "Picher Canyon"
      },
      "geometry": {
        "coordinates": [
          [
            [
              -122.68022793124464,
              37.92623346660906
            ],
            [
              -122.67525457788447,
              37.92918536531603
            ],
            [
              -122.66993653666756,
              37.93345764038591
            ],
            [
              -122.66166291707097,
              37.93761496334882
            ],
            [
              -122.65860996748364,
              37.938935359821784
            ],
            [
              -122.65959478993113,
              37.94173141522599
            ],
            [
              -122.66273771405922,
              37.94464553711123
            ],
            [
              -122.66655357921972,
              37.942149304718654
            ],
            [
              -122.67244246901976,
              37.93926293973665
            ],
            [
              -122.67648359235552,
              37.93620488916644
            ],
            [
              -122.67808012625633,
              37.934973131982005
            ],
            [
              -122.67929974658648,
              37.933890012135464
            ],
            [
              -122.68124931076369,
              37.932050441545336
            ],
            [
              -122.68297273375572,
              37.93015790884209
            ],
            [
              -122.6819313892115,
              37.92743796393317
            ],
            [
              -122.68022793124464,
              37.92623346660906
            ]
          ]
        ],
        "type": "Polygon"
      },
      "id": 0
    },
    {
      "type": "Feature",
      "properties": {
        "Canyon": "Garden Club Canyon"
      },
      "geometry": {
        "coordinates": [
          [
            [
              -122.68297973302839,
              37.93014059391763
            ],
            [
              -122.68230354591371,
              37.930913947218784
            ],
            [
              -122.68124418609543,
              37.93209618426552
            ],
            [
              -122.6780659646859,
              37.93502229516338
            ],
            [
              -122.67246359328325,
              37.939242195246905
            ],
            [
              -122.66660055929515,
              37.94209132669272
            ],
            [
              -122.66276296049199,
              37.94463854927042
            ],
            [
              -122.66806046126082,
              37.94895782713773
            ],
            [
              -122.67587828682295,
              37.94173552108853
            ],
            [
              -122.67984380558636,
              37.938683766974066
            ],
            [
              -122.68345966817802,
              37.93468595587379
            ],
            [
              -122.68684954712069,
              37.931860925157594
            ],
            [
              -122.68780894682179,
              37.93052404242286
            ],
            [
              -122.6846749077992,
              37.930675388858035
            ],
            [
              -122.68361461525504,
              37.93059602316703
            ],
            [
              -122.68297973302839,
              37.93014059391763
            ]
          ]
        ],
        "type": "Polygon"
      },
      "id": 1
    },
    {
      "type": "Feature",
      "properties": {
        "Canyon": "Volunteer Canyon"
      },
      "geometry": {
        "coordinates": [
          [
            [
              -122.68010656477827,
              37.92630481530419
            ],
            [
              -122.68066505094933,
              37.92617524607297
            ],
            [
              -122.67281339242501,
              37.92166609460945
            ],
            [
              -122.66239785376217,
              37.92961298046636
            ],
            [
              -122.65663127838273,
              37.933882045215896
            ],
            [
              -122.6552655105297,
              37.93503903875616
            ],
            [
              -122.65855347017586,
              37.93898863812652
            ],
            [
              -122.65980292135993,
              37.938389953539826
            ],
            [
              -122.66122575826408,
              37.937786643494434
            ],
            [
              -122.66280158838353,
              37.937038532160415
            ],
            [
              -122.66621333708868,
              37.93533715384294
            ],
            [
              -122.66989854015429,
              37.933490427711206
            ],
            [
              -122.67179565602626,
              37.93193377394559
            ],
            [
              -122.67332677922954,
              37.93070009326944
            ],
            [
              -122.67523919442306,
              37.92917958341796
            ],
            [
              -122.67744229407631,
              37.927876264487935
            ],
            [
              -122.68010656477827,
              37.92630481530419
            ]
          ]
        ],
        "type": "Polygon"
      },
      "id": 2
    },
    {
      "type": "Feature",
      "properties": {
        "Canyon": "Pike County Gulch"
      },
      "geometry": {
        "coordinates": [
          [
            [
              -122.69236899781481,
              37.93314184384346
            ],
            [
              -122.68990561266705,
              37.93131827875595
            ],
            [
              -122.68773493042505,
              37.930452141500325
            ],
            [
              -122.68679004521366,
              37.931922554519346
            ],
            [
              -122.68587751229762,
              37.93265975974023
            ],
            [
              -122.68516246402945,
              37.93328416654887
            ],
            [
              -122.68342591823605,
              37.93469409789846
            ],
            [
              -122.68111171193566,
              37.93730549449812
            ],
            [
              -122.67986037746654,
              37.9386750675412
            ],
            [
              -122.67593969662371,
              37.94166246661321
            ],
            [
              -122.66814612851616,
              37.9489450475637
            ],
            [
              -122.67364433067425,
              37.951681090857676
            ],
            [
              -122.69236899781481,
              37.93314184384346
            ]
          ]
        ],
        "type": "Polygon"
      },
      "id": 3
    }
  ]
}
"""
#endregion

#region create geodataframe
gdf = gpd.read_file(geojson_string, driver='GeoJSON')

# Create a GeoDataFrame from merged_data
gdf_merged = gpd.GeoDataFrame(merged_data, geometry=gpd.points_from_xy(merged_data['Longitude'], merged_data['Latitude']))
#endregion
# Function to determine canyon based on coordinates

#region get_canyon function, create Canyon column
def get_canyon(row):
    for index, canyon_row in gdf.iterrows():
        if row['geometry'].within(canyon_row['geometry']):
            return canyon_row['Canyon']  # Use the actual column name for canyon names
    return 'Unknown Canyon'

# Apply the function to create the 'Canyon' column in merged_data
gdf_merged['Canyon'] = gdf_merged.apply(lambda row: get_canyon(row), axis=1)

# Assign 'Canyon' column back to merged_data
merged_data['Canyon'] = gdf_merged['Canyon']
#endregion


#region Export the merged_data DataFrame to an Excel file in the same folder
merged_data.to_excel('merged_data.xlsx', index=False)
print("Merged data has been exported to merged_data.xlsx")
#endregion