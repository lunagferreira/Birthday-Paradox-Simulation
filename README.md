# Birthday Paradox Simulation

## Overview

This project simulates the Birthday Paradox to understand the probability of two people sharing the same birthday in a group. It was created to explore this interesting probability problem using Scala for data processing and analysis. The tasks include generating JSON files, processing gender data, and creating a detailed Excel summary report.

## Dataset

The project utilizes a JSON file that contains simulation results of the Birthday Paradox. The dataset includes information such as the day of the year, timestamp, and how long ago the event was recorded.

### Files Included:
- `BirthdayParadox.json`

## Features

1. **Year-wise Data Files**: Generates separate files for each year, containing the simulation results for that year.
2. **2023 Texas Crimes**: Creates a DataFrame with crimes committed in Texas in 2023, modifies gender data to full words, and replaces null gender values with "N/A".
3. **Excel Summary Report**: Writes an Excel file with three sheets:
   - **Sheet 1**: Summary of events by day.
   - **Sheet 2**: Breakdown by day and event count.
   - **Sheet 3**: Further breakdown including detailed event information.

## Technologies Used

- **Scala**: For data processing and analysis.
- **Apache Spark**: For handling large-scale data processing.
- **Pandas**: For data manipulation and preprocessing.
- **Openpyxl**: For writing Excel files.
