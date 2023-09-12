# Bank Market Capitalization ETL

**Description:**

This ETL (Extract, Transform, Load) script is designed to automate the process of data integration specific to the banking sector's market capitalization information. The primary objective is to ensure accurate data transformation and enhance the data's usability.

**Detailed Workflow:**

1. **Extraction:** 
   - The script begins by downloading the necessary data files which include the bank's market capitalization in USD and the prevailing exchange rates.
   - It then reads the JSON data files which house the bank's market capitalization details.
  
2. **Transformation:** 
   - The exchange rate for GBP is retrieved from a CSV file.
   - The market capitalization, which is initially in USD, is then converted to GBP using the extracted exchange rate. This ensures that the financial data is represented in a currency that is potentially more relevant to the end-user or the business scenario at hand.
   - The transformed data is also rounded for better presentation and to maintain data consistency.

3. **Loading:** 
   - Finally, the transformed data is saved into a new CSV file. This CSV file can be integrated into various financial systems or used for data analysis, visualization, or reporting purposes.

This ETL script emphasizes data accuracy, efficiency, and ensures that the transformed data is ready for further financial analysis or any other relevant operations.


