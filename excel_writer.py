
import openpyxl

# Load the workbook
workbook = openpyxl.load_workbook("/Users/indus/01_Projects/opencode-sandbox/opencode/Precision Camshafts 19072025.xlsx")

# Select the active sheet
sheet = workbook.active

# Write to cell A1
sheet["A1"] = "test"

# Save the workbook
workbook.save("/Users/indus/01_Projects/opencode-sandbox/opencode/Precision Camshafts 19072025.xlsx")
