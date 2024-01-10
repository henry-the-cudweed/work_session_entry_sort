#CREATE PIVOT_WORK_SESSION_CREATED.XLSX
#then generate organized excel sheet

#planning_sheet is from a csv i have with some data on individual patches

#work_session_entry.csv is created by copy/pasting the CF work session entry lookup page
#into an excel sheet and converting to CSV
# expected column headers: ,Project,Date,Activity,Crew,Role,# People,Person-Hours,Reference,Notes,

#calflora-out.csv exported from group observations

import os
import pandas as pd
import time
import datetime
import re

# Specify the file names for the existing files
pivot_work_session_file = 'pivot_work_session.xlsx'

# Check if the files exist and delete them if they do
if os.path.exists(pivot_work_session_file):
    os.remove(pivot_work_session_file)
    print(f"Previous file {pivot_work_session_file} has been deleted.")

# Assuming you have the WorkSessionEntry DataFrame
# Replace the following line with your actual DataFrame or loading logic
work_session_entry = pd.read_csv('work_session_entry.csv')  # Replace with your file or DataFrame

# Convert 'Person-Hours' column in work_session_entry to numeric in case it's not already
work_session_entry['Person-Hours'] = pd.to_numeric(work_session_entry['Person-Hours'], errors='coerce')

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

# Load the pivot_work_session_created.xlsx file
pivot_work_session_file = 'pivot_work_session.xlsx'
pivot_work_session = pd.read_excel(pivot_work_session_file)

# Assuming you have the PlanningSheet DataFrame
# Replace the following line with your actual DataFrame or loading logic
planning_sheet = pd.read_csv('planning_sheet.csv')  # Replace with your file or DataFrame

# Extract columns from the PlanningSheet DataFrame based on the 'Reference' column
reference_columns = ['Canyon','Status', 'Next return','state of patch', 'est person-hours remaining']

# Get the date columns from pivot_work_session
date_columns = [col for col in pivot_work_session.columns if pivot_work_session[col].dtype == 'datetime64[ns]']

# Create sort_date function
# return 1 brings oldest-newest
# return -1 brings newest-oldest
def sort_date(value):
    try:
        return 1 * time.mktime(datetime.datetime.strptime(value, "%m/%d/%Y").timetuple())
    except:
        return float("-inf")

# Define the desired column order
other_columns = list(pivot_work_session.columns.difference(date_columns + ['Reference', 'Total Hours'] + reference_columns))

# Sort other_columns into organized date_columns
date_columns = sorted(other_columns, key=sort_date)

column_order = ['Reference', 'Total Hours'] + reference_columns + date_columns

print("date columns", date_columns)

# Reorganize columns
merged_data = pd.merge(pivot_work_session, planning_sheet[['Reference'] + reference_columns], on='Reference', how='left')
merged_data = merged_data[column_order]

# add Calflora links
url_prefix = 'https://www.calflora.org/entry/poe.html#vrid='
merged_data['Link'] = url_prefix + merged_data['Reference']
merged_data = merged_data [['Link'] + column_order]

# Assuming you have the calflora_out DataFrame
# Replace the following line with your actual DataFrame or loading logic
calflora_out = pd.read_csv('calflora-out.csv')  # Replace with your file or DataFrame

# Select the desired columns
calflora_columns = ["ID",'Gross Area', 'Common Name', 'Percent Cover']
calflora_data = calflora_out[calflora_columns]

# Merge calflora_data with merged_data using the correct columns
merged_data = pd.merge(merged_data, calflora_data, left_on='Reference', right_on='ID', how='left')

# Drop the redundant 'ID' column after merging
merged_data.drop('ID', axis=1, inplace=True)

# Extract numerical values from "Percent Cover" column and convert to numeric
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

merged_data = merged_data[['Canyon','Common Name','Link','Reference', 'Total Hours','Next return',  'est person-hours remaining','Status', 'state of patch','Gross Area',  'Percent Cover', 'Est Infested Cover Range'] + date_columns]

# Export the merged_data DataFrame to an Excel file in the same folder
merged_data.to_excel('merged_data.xlsx', index=False)
print("Merged data has been exported to merged_data.xlsx")
