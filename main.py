import pandas as pd
import requests
from IPython.display import display
from concurrent.futures import ThreadPoolExecutor
from terminal import *
import warnings
import psutil
import time
import gc
import os
import subprocess
warnings.filterwarnings('ignore')

# jdk_path = Path("C:/Program Files/Java/jdk-17/bin")  # Update this to the path of your JDK bin
_thetadata_jar = Path(__file__).parent / "ThetaTerminal.jar" # Update to the relative path of ThetaTerminal.jar

def is_terminal_instance_running() -> bool:
    """Checks if the Theta Data Terminal is already running."""
    for proc in psutil.process_iter(['cmdline']):
        cmdline = proc.info.get('cmdline')
        if cmdline and _thetadata_jar.name in cmdline:
            return True
    return False


def launch_terminal(username: str = None, passwd: str = None, jvm_mem: int = 4):
    """Launches the Theta Data Terminal in the background."""
    if not _thetadata_jar.exists():
        print(f"{_thetadata_jar} not found. Ensure the JAR file is in the specified relative location.")
        return

    java_executable = jdk_path / "java"
    command = [str(java_executable), f"-Xmx{jvm_mem}G", "-jar", str(_thetadata_jar), username or "", passwd or ""]

    try:
        # Redirect stdout and stderr to DEVNULL to run in the background
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
        print("Theta Data Terminal launched in the background.")
    except Exception as e:
        print(f"Failed to launch ThetaTerminal: {e}")


def kill_existing_terminal() -> None:
    """
    Utility function to kill any ThetaData terminal processes by scanning all running proceeses
    and killing such process
    """
    for pid in psutil.pids():
        try:
            cmdline_args = psutil.Process(pid=pid).cmdline()
            for arg in cmdline_args:
                if _thetadata_jar in arg:
                    os.kill(pid, signal.SIGTERM)
        except:
            pass


def convert_ms_to_time(ms_of_day):
  """Converts milliseconds of the day to HH:MM:SS format.

  Args:
    ms_of_day: Milliseconds of the day.

  Returns:
    A string representing the time in HH:MM:SS format.
  """
  seconds = ms_of_day // 1000
  minutes = seconds // 60
  hours = minutes // 60
  seconds %= 60  #seconds = seconds%60 
  minutes %= 60

  return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)


def forward4_expdate_dic(symbol,earn_date):
  '''
  It will take arg symbol and eaning data

  return
  immediate 4 expiry dates from earning date
  '''

  url = "http://127.0.0.1:25510/v2/list/expirations"
  querystring = {"root":symbol}
  headers = {"Accept": "application/json"}
  response = requests.get(url, headers=headers, params=querystring)
  #print(response.json())
  data=response.json()
  expiry_dates=data['response'] # expiry dates are in list 
  greater_4exp_dates = list(filter(lambda date: str(date) > earn_date, expiry_dates))
  
  expdtlst = greater_4exp_dates[0:4]
  return expdtlst


def start_end_date(symbol,earn_date,greater_4exp_dates):
  '''
  It will take symbol ,(4 expiry date immediate to earning date) and earning date as arg

  return 

  immediate back date from  earn date and next immediate date as start and end date respectively. 
  '''
  exp_date=greater_4exp_dates[0] # immediate expiry date
  url = "http://127.0.0.1:25510/v2/list/dates/option/quote"             
  querystring = {"root":symbol,"exp":str(exp_date)}
  headers = {"Accept": "application/json"}
  response = requests.get(url, headers=headers, params=querystring)
  data=response.json()
  dates=data['response']
  greater_dates = list(filter(lambda date: str(date) > earn_date, dates))
  lesser_dates = list(filter(lambda date: str(date) < earn_date, dates))

  if len(lesser_dates)==0:
      print(f"lesser dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date} ")
      return "no data found" 
  else:
      start_date=lesser_dates[-1]                 
  if len(greater_dates)==0:
      print(f"greater dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date}")
      return "no data found"

  else:
      end_date=greater_dates[0]
       
  return start_date,end_date


def bulk_ohlc_data(symbol,earn_date,greater_4exp_dates,start_date,end_date):
  url = "http://127.0.0.1:25510/v2/bulk_hist/option/ohlc"
  rows = []
  
  for exp_date in greater_4exp_dates:
      querystring = {"exp":str(exp_date),"start_date":str(start_date),"end_date":str(end_date),"root":symbol,"ivl":"900000"}
      headers = {"Accept": "application/json"}
      response = requests.get(url, headers=headers, params=querystring)

      data = response.json()
          # Extract ticks and create a list of dictionaries for the DataFrame
      for entry in data['response']:
          for tick in entry['ticks']:
              row = {
                  'Symbol': symbol,
                  'Earn Date': earn_date,
                  'ms_of_day': tick[0],
                  'open': tick[1],
                  'high': tick[2],
                  'low': tick[3],
                  'close': tick[4],
                  'volume': tick[5],
                  'count': tick[6],
                  'date': tick[7],
                  'contract_root': entry['contract']['root'],
                  'contract_expiration': entry['contract']['expiration'],
                  'contract_strike': entry['contract']['strike'],
                  'contract_right': entry['contract']['right']
              }
              rows.append(row)

  # Create a DataFrame
  ohlc_df = pd.DataFrame(rows)
  #df.to_excel("D:\\Thetadata\\ohlcdatare.xlsx",index=False)
  return ohlc_df


def greeks_data(ohlc_df,symbol, earn_date, greater_4exp_dates,startdate,enddate):
    dataframes = []
    
    for exp_date in greater_4exp_dates:
        exp_df=ohlc_df[ohlc_df['contract_expiration']==exp_date]

        unique_dates=exp_df.date.unique()
        for date in unique_dates:
          date_df=exp_df[exp_df['date']==date]
      
          uniqueop=date_df.contract_right.unique()
          for op in uniqueop:
            op_df=date_df[date_df['contract_right']==op]

            lst_strikes=op_df.contract_strike.unique()
            for strike in lst_strikes:
              url = "http://127.0.0.1:25510/v2/hist/option/greeks"
              querystring = {"root":symbol,"exp":exp_date,"right":op,"strike":strike,"start_date":date,"end_date":date,"ivl":"900000"}
              headers = {"Accept": "application/json"}
              response = requests.get(url, headers=headers, params=querystring)
              
              data = response.json()          
                
              # Extract the column names from the header
              columns = data['header']['format']  # This will give you the list of column names
              # Extract the response data
              response_data = data['response']  # This is the list of data points
              
              df = pd.DataFrame(response_data, columns=columns)
              #'ms_of_day','date','contract_root','contract_expiration','contract_strike','contract_right'
              
              df['contract_right'] = op  # Add a column for the option type (C or P)
              df['contract_strike'] = strike  # Add a column for the strike price
              df['contract_expiration'] = exp_date  # Add a column for the expiration date
              df['contract_root'] = symbol  # Add a column for the root symbol
              
              dataframes.append(df)  # Append the DataFrame to the list
    


    greeks_df = pd.concat(dataframes, ignore_index=True)
    #final_df.to_excel("D:\\Thetadata\\greeksdatare.xlsx",index=False)
    #display(final_df)  # Display the final DataFrame
    return greeks_df


def run(symbol, earning_date):
        # earning_date = str(earning_date)

        print(f"symbol {symbol} and earn_date {earning_date}")
      
        t1=time.perf_counter() # timer start

        greater_4exp_dates = forward4_expdate_dic(symbol,earning_date)# calculating 4 forward expiry dates
        print(f"expiry dates {greater_4exp_dates}")

        result = start_end_date(symbol,earning_date,greater_4exp_dates)               
        if result == "no data found":
          print("Not processing  this case ")
        else:
            start_date,end_date = result
            print(f'start and end date is {result}')
                      
            ohlc_df = bulk_ohlc_data(symbol,earning_date,greater_4exp_dates,start_date,end_date)
          
            greek_df = greeks_data(ohlc_df,symbol, earning_date, greater_4exp_dates,start_date,end_date)
            
            main_df=ohlc_df.merge(greek_df,how='left',on = ['ms_of_day','date','contract_root','contract_expiration','contract_strike','contract_right'])
            main_df['time']=main_df['ms_of_day'].apply(convert_ms_to_time)
            
            #display(main_df.head())
            output_file = r"C:/Users/ASUS/Desktop/git repo theta data/Output Data"
            symbol_name_and_earning_date = f"{symbol}-{earning_date}.xlsx"
            output_path = os.path.join(output_file,symbol_name_and_earning_date)
            
            main_df.to_excel(output_path,index=False)
            #print(f"data converted into excel")
            del ohlc_df, greek_df, main_df
            gc.collect()

        print(f"congratulation you got the output for {symbol}-{earning_date}")
        t2=time.perf_counter()
        print(f"Elapsed time = {(t2-t1)/60}")
#run('AAPL','20240102') 


def call_thread():
  with ThreadPoolExecutor(max_workers=500) as executor:
      directory = r"C:/Users/ASUS/Desktop/git repo theta data/Data Extracted Quarter vice"  # Use raw string for path
      filename = 'df_2_2023.xlsx'  # The filename you want to save as

      # Create the full path
      output_filepath = os.path.join(directory, filename)
      df = pd.read_excel(output_filepath)     
      # df['Earning Date']=pd.to_datetime(df['Earning Date'],format='%d-%m-%Y').dt.strftime(%Y%m%d)  
      future = executor.map(run,df['Symbol'],df['Earnings_Date']) # Earning Date

      
if not is_terminal_instance_running():
      print("Starting Theta Terminal...")
      launch_terminal(username="suresh2398kumar@gmail.com", passwd="$India23", jvm_mem=4)

if __name__ == '__main__':
  start = time.perf_counter()
  run('ACB','20240208')
  # call_thread()
  end = time.perf_counter()
  print(f"\nTotal elapsed Time df: {round((end-start)/60,2)} min")   
        
