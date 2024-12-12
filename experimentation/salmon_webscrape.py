import aiohttp
import asyncio
import pandas as pd
from tqdm.asyncio import tqdm  # Using tqdm's asyncio integration
import io
from datetime import datetime

# Remove nest_asyncio unless you're running this in an environment that already has an event loop (e.g., Jupyter)
# import nest_asyncio
# nest_asyncio.apply()

async def fetch_data(session, url, semaphore, retries=3):
    """
    Fetch data from a URL with retry logic.

    :param session: The aiohttp session.
    :param url: The URL to fetch.
    :param semaphore: Semaphore to limit concurrent requests.
    :param retries: Number of retry attempts for failed requests.
    :return: DataFrame if successful, None otherwise.
    """
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        return pd.read_csv(io.StringIO(text))
                    else:
                        print(f"Attempt {attempt}: Failed to fetch {url} with status {response.status}")
            except aiohttp.ClientError as e:
                print(f"Attempt {attempt}: Client error for {url}: {e}")
            except asyncio.TimeoutError:
                print(f"Attempt {attempt}: Timeout error for {url}")
            except Exception as e:
                print(f"Attempt {attempt}: Unexpected error for {url}: {e}")
            
            # Wait before retrying
            await asyncio.sleep(2)
        
        print(f"Failed to fetch {url} after {retries} attempts.")
        return None

async def main():
    """
    Main asynchronous function to fetch data concurrently and compile into a single DataFrame.
    """
    dfs = []
    semaphore = asyncio.Semaphore(50)  # Limit to 50 concurrent requests
    timeout = aiohttp.ClientTimeout(total=60)  # Set a timeout for requests

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for i in range(220000, 229999):
            # Corrected URL with proper encoding of double quotes around the stock_number
            url = f"https://data.wa.gov/resource/fgyz-n3uk.csv?$where=stock_number=%{i}%22"
            tasks.append(fetch_data(session, url, semaphore))
        
        # Using tqdm's asyncio integration for the progress bar
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Downloading Data"):
            df = await task
            if df is not None:
                dfs.append(df)
    
    if dfs:
        # Combine all DataFrames into one
        combined_df = pd.concat(dfs, ignore_index=True)
        # Save to a CSV file
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_name = f"salmon_data_{current_date}.csv"
        combined_df.to_csv(file_name, index=False)
        print(f"Data successfully downloaded and saved to '{file_name}'.")
    else:
        print("No data was fetched.")

if __name__ == "__main__":
    asyncio.run(main())