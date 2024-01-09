#CREATE PIVOT_WORK_SESSION_CREATED.XLSX
#then generate organized excel sheet
#planning_sheet is from a csv i have with some data on individual patches
#work_session_entry.csv is created by copy/pasting the CF work session entry lookup page
#into an excel sheet and converting to CSV
# expected column headers: ,Project,Date,Activity,Crew,Role,# People,Person-Hours,Reference,Notes,

import os
import pandas as pd
import time
import datetime

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
reference_columns = ['Est Covered Patch Size', 'state of patch', 'est person-hours remaining', 'Species', 'Canyon', 'Status', 'Next return']

# Get the date columns from pivot_work_session
date_columns = [col for col in pivot_work_session.columns if pivot_work_session[col].dtype == 'datetime64[ns]']

#Create sort_date function
#return 1 brings oldest-newest
#return -1 brings newest-oldest
def sort_date(value):
  try:
    return 1 * time.mktime(datetime.datetime.strptime(value, "%m/%d/%Y").timetuple())
  except:
    return float("-inf")
  


# Define the desired column order
other_columns = list(pivot_work_session.columns.difference(date_columns + ['Reference', 'Total Hours'] + reference_columns))

#sort other_columns into organized date_columns
date_columns = sorted(other_columns, key=sort_date)

column_order = ['Reference', 'Total Hours']  + reference_columns + date_columns

print("date columns", date_columns)


# Reorganize columns
merged_data = pd.merge(pivot_work_session, planning_sheet[['Reference'] + reference_columns], on='Reference', how='left')
merged_data = merged_data[column_order]

#add Calflora links
url_prefix = 'https://www.calflora.org/entry/poe.html#vrid='
merged_data['Link'] = url_prefix + merged_data['Reference']
merged_data = merged_data [['Link'] + column_order]


# Specify the path where you want to save the Excel file with a specific name
output_file_path = 'merged_pivot_work_session.xlsx'

# Export the merged_data DataFrame to an Excel file
merged_data.to_excel(output_file_path, index=False)

print(f"Data has been merged and exported to {output_file_path}")