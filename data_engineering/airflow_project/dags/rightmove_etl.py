# Import the required packages.
import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import numpy as np
from datetime import date, datetime, timedelta

# Define and create the scraper to pull the data.
class RightmoveScraper:

    results = []

    def fetch(self, url):
        print('HTTP GET request to URL: %s' % url, end='')
        response = requests.get(url)
        print(' | Status code: %s' % response.status_code)

        return response

    def parse(self, html):
        content = BeautifulSoup(html, 'html.parser')

        title = [title.text.strip() for title in content.findAll('h2', {'class': 'propertyCard-title'})]
        Property_Address = [address['content'] for address in content.findAll('meta', {'itemprop': 'streetAddress'})]
        # description = [description.text.replace('*', '') for description in content.findAll('span', {'data-test': 'property-description'})]
        price_month = [price.text.strip()[1:] for price in content.findAll('div', {'class': 'propertyCard-rentalPrice-primary'})]
        price_week = [price_w.text.strip()[1:] for price_w in content.findAll('span', {'class': 'propertyCard-secondaryPriceValue'})]
        agent_name = [ agent_name.text.strip()[3:] for agent_name in content.findAll('span', {'class': 'propertyCard-branchSummary-branchName'})]
        Agent_Number = [ agent_number.text for agent_number in content.findAll('a', {'data-test': 'contact-agent-phone-number'})]
        Created_Date = [date.text.split(' ')[-1]  for date in content.findAll('span', {'class': 'propertyCard-branchSummary-addedOrReduced'})]
        
        for index in range(0, len(title)):
            self.results.append({
                'title' : title[index],
                'Property_Address': Property_Address[index],
                # 'description': description[index],
                'price_month': price_month[index],
                'price_week': price_week[index],
                'agent_name': agent_name[index],
                'Agent_Number': Agent_Number[index],
                'Created_Date': Created_Date[index],
            })
        

    def run(self):
        for page in range(0, 43):
            index = page * 24
            url = 'https://www.rightmove.co.uk/student-accommodation/find.html?locationIdentifier=REGION%5E87490&radius=40.0&index=' + str(index) + '&propertyTypes=&mustHave=&dontShow=&furnishTypes=&keywords=' 
           
       
            response = self.fetch(url)
            self.parse(response.text)
        self.to_csv()

    def to_csv(self):
        with open('/home/sirmuguna/projects/personal_projects/data_engineering/airflow_project/data_csv/rightmove.csv', 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.results[0].keys())
            writer.writeheader()

            for row in self.results:
                writer.writerow(row)

            print('stored results to "rightmove.csv"')

if __name__ == '__main__':
    scraper = RightmoveScraper()
    scraper.run()
    
#Define the csv file path.
file_path = '/home/sirmuguna/projects/personal_projects/data_engineering/airflow_project/data_csv/rightmove.csv'

# Read the CSV file with 'latin1' encoding
data = pd.read_csv(file_path, encoding='latin1')

df = pd.DataFrame(data)
# Remove rows with NaN values
df_cleaned = df.dropna()
print(df_cleaned.columns)
   
# List of values to remove and move to the "Property Type" column
values_to_remove = ['Flat share', 'House share', 'Property', 'Parking', 'Studio Apartment', 'Studio flat', 'Detached house']

# Function to remove and move values
def remove_and_move(row):
    if row['title'] in values_to_remove:
        df_cleaned.loc[row.name, 'title'] = None
        df_cleaned.loc[row.name, 'Property Type'] = row['title']
    else:
        df_cleaned.loc[row.name, 'title'] = row['title']
        df_cleaned.loc[row.name, 'Property Type'] = None

# Apply the function to the DataFrame
df_cleaned.apply(remove_and_move, axis=1)

# Define a function to split the "title" column
def split_title(row):
    if row['title'] is None:
        df_cleaned.loc[row.name, 'Number_of_bedroom(s)'] = None
        df_cleaned.loc[row.name, 'Property_Type'] = None
    else:
        parts = row['title'].split(' bedroom ', 1)
        if len(parts) == 2:
            df_cleaned.loc[row.name, 'Number_of_bedroom(s)'] = int(parts[0])
            df_cleaned.loc[row.name, 'Property_Type'] = str(parts[1])
        else:
            df_cleaned.loc[row.name, 'Number_of_bedroom(s)'] = row['title']
            df_cleaned.loc[row.name, 'Property_Type'] = None

# Apply the function to the DataFrame
df_cleaned.apply(split_title, axis=1)

# Drop the original "title" column
df_cleaned.drop(columns=['title'], inplace=True)

# Replace NaN records in 'Property_Type' with values from 'Property Type'
df_cleaned['Property_Type'].fillna(df_cleaned['Property Type'], inplace=True)

# Remove rows with NaN values in the 'Property_Type' column
df_cleaned = df_cleaned.dropna(subset=['Property_Type'])

# Capitalize the values in the 'Property_Type' column
df_cleaned['Property_Type'] = df_cleaned['Property_Type'].str.capitalize()

# Drop the 'Property Type' column
df_cleaned.drop(columns=['Property Type'], inplace=True)

# Define a dictionary to specify replacements
replace_dict = {
    'Studio apartment': 'Not Available',
    'House of multiple occupation': 'Not Available'
}

# Replace values in 'Number_of_bedroom(s)' column
df_cleaned['Number_of_bedroom(s)'] = df_cleaned['Number_of_bedroom(s)'].replace(replace_dict)

# Replace NaN with '0'
df_cleaned['Number_of_bedroom(s)'].fillna('0', inplace=True)

# Convert the 'Number_of_bedroom(s)' column to integer
df_cleaned['Number_of_bedroom(s)'] = df_cleaned['Number_of_bedroom(s)'].astype(int)

#Cleaning and transforming the column 'price_month' & 'price_week' columns.
# Extract values before the first space delimiter and rename the column
df_cleaned['Price_Month'] = df_cleaned['price_month'].str.split(pat=' ', n=1).str[0].str.replace(',', '')

# Convert the "Price_Month" column to integer & drop the previous column.
df_cleaned['Price_Month'] = df_cleaned['Price_Month'].astype(int)
df_cleaned.drop(columns=['price_month'], inplace=True)

df_cleaned['Price_Week'] = df_cleaned['price_week'].str.split(pat=' ', n=1).str[0].str.replace(',', '')
# Convert the "Price_Week" column to integer & drop the previous column.
df_cleaned['Price_Week'] = df_cleaned['Price_Week'].astype(int)
df_cleaned.drop(columns=['price_week'], inplace=True)

# Split the "agent_name" column on the ',' delimiter
split_data = df_cleaned['agent_name'].str.split(', ', expand=True)

# Rename the split columns
split_data.columns = ['Agent_Name', 'Agent_Location', 'Unused_Column']

# Combine the split data with the original DataFrame
df_cleaned = pd.concat([df_cleaned, split_data], axis=1)

# Drop the original "agent_name" and the unused column if needed
df_cleaned.drop(columns=['agent_name', 'Unused_Column'], inplace=True)

# Define a function to convert 'today' and 'yesterday' to dates
def convert_special_dates(date_str):
    if date_str == 'today':
        return (datetime.now()).strftime('%d-%m-%Y')
    elif date_str == 'yesterday':
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime('%d-%m-%Y')
    else:
        # If the date is not 'today' or 'yesterday', assume it's already in the correct format
        return date_str

# Apply the conversion function to the 'Created_Date' column in the DataFrame
df_cleaned['Created_Date'] = df_cleaned['Created_Date'].apply(convert_special_dates)

# List of columns to capitalize
columns_to_capitalize = ['Property_Address', 'Property_Type', 'Agent_Name', 'Agent_Location']

# Capitalize the values in the specified columns
for column in columns_to_capitalize:
    df_cleaned[column] = df_cleaned[column].str.title()

# Remove trailing spaces in specified columns only if they are of string type
columns_to_strip = ['Created_Date', 'Property_Type', 'Property_Address', 'Number_of_bedroom(s)', 'Price_Month', 'Price_Week', 'Agent_Name', 'Agent_Location', 'Agent_Number']

for column in columns_to_strip:
    if df_cleaned[column].dtype == 'object':  # Check if the column contains string data
        df_cleaned[column] = df_cleaned[column].str.strip()

# Rearrange the columns in the dataframe.
df_cleaned = df_cleaned[['Created_Date', 'Property_Type', 'Property_Address', 'Number_of_bedroom(s)', 'Price_Month', 'Price_Week', 'Agent_Name', 'Agent_Location', 'Agent_Number']]

# Save the modified DataFrame to a CSV file
df_cleaned.to_csv('/home/sirmuguna/projects/personal_projects/data_engineering/airflow_project/data_csv/rightmove_london_cleaned_data.csv', index=False)
