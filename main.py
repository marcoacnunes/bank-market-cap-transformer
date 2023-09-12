import pandas as pd

def download_files():
    urls = [
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json",
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json",
        "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv"
    ]
    for url in urls:
        os.system(f"wget {url}")

# JSON Extract Function
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    desired_columns = ['Name','Market Cap (US$ Billion)']
    dataframe = dataframe[desired_columns]
    return dataframe

def extract():
    return extract_from_json("bank_market_cap_1.json")

def transform_to_gbp(df, exchange_rate):
    df['Market Cap (US$ Billion)'] = df['Market Cap (US$ Billion)'] * exchange_rate
    df['Market Cap (US$ Billion)'] = df['Market Cap (US$ Billion)'].round(3)
    df.rename(columns={'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'}, inplace=True)
    return df

def load(dataframe, filename="bank_market_cap_gbp.csv"):
    dataframe.to_csv(filename, index=False)

def log(message):
    print(message)

if __name__ == "__main__":
    log("ETL Job Started")

    log("Downloading necessary files")
    download_files()
    
    log("Extract phase Started")
    df = extract()
    log("Extract phase Ended")
    
    df_exchange = pd.read_csv("exchange_rates.csv", index_col=0)
    exchange_rate = df_exchange.loc["GBP", "Rates"]
    
    log("Transform phase Started")
    df_transformed = transform_to_gbp(df, exchange_rate)
    log("Transform phase Ended")
    
    log("Load phase Started")
    load(df_transformed)
    log("Load phase Ended")
    
    log("ETL Job Completed")