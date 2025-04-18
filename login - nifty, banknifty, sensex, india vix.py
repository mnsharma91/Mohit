import requests
import pandas as pd
import json
import xlwings as xw
import time
from datetime import datetime
import openpyxl
import matplotlib.pyplot as plt
from pprint import pprint

tdate = datetime.now().date()


try:
    with open(f'access_token/{tdate}_access_code.json', 'r') as file_read:
        access = json.load(file_read)
except:
    api_key = 'xxxxxxxxxxxxxxxxxx'
    api_secret = 'xxxxxxxxxxx'
    uri = 'https://www.google.com/'
    url1 = f'https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={uri}'
    print(url1)

    code = input('Enter the Code')
    url = 'https://api.upstox.com/v2/login/authorization/token'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'code': code,
        'client_id': api_key,
        'client_secret': api_secret,
        'redirect_uri': uri,
        'grant_type': 'authorization_code',
    }

    response = requests.post(url, headers=headers, data=data)
    access = response.json()['access_token']
    print(response.status_code)
    print(response.json())

    with open(f'access_token/{tdate}_access_code.json', 'w') as file_write:
        json.dump(access, file_write)

#############################################################################

def instrument():
    inst_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    instrument = pd.read_csv(inst_url)
    instrument.to_csv('instrument.csv')

yn = int(input('Do you Want to Update Instrument : 0 / 1 : '))

if yn==1:
    instrument()
    print("Instrument Data Updated Successfully")


df = pd.read_csv('instrument.csv', index_col=0)

df_niftyoptions = df[(df['exchange'] == 'NSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'NIFTY')]
expiry_list_nifty = df_niftyoptions['expiry'].unique().tolist()
expiry_list_nifty.sort()

df_bnf = df[(df['exchange'] == 'NSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'BANKNIFTY')]
expiry_list_bnf = df_bnf['expiry'].unique().tolist()
expiry_list_bnf.sort()

df_sensex = df[(df['exchange'] == 'BSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'SENSEX')]
expiry_list_sensex = df_sensex['expiry'].unique().tolist()
expiry_list_sensex.sort()

wb = xw.Book('Analysis.xlsm')
summary = wb.sheets('summary')
nifty_0 = wb.sheets('nifty_0')
nifty_1 = wb.sheets('nifty_1')
nifty_3 = wb.sheets('nifty_3')
bnf_0 = wb.sheets('bnf_0')
sensex_0 = wb.sheets('sensex_0')
breakpoint()
instrument_key_nifty = 'NSE_INDEX|Nifty 50'
instrument_key_bnf = 'NSE_INDEX|Nifty Bank'
instrument_key_sensex = 'BSE_INDEX|SENSEX'

structure_initial = {}
structure_current = {}
past_data={}

a=b=c=d=e=f=1
initialize=1

def check_data(initial_data, current_data):

    global expiry_name_0, expiry_name_1, expiry_name_2
    
    initial_df = pd.DataFrame(initial_data).reset_index(drop=True)
    current_df = pd.DataFrame(current_data).reset_index(drop=True)

    df_concat = pd.concat([initial_df, current_df], axis=1)
    re_order = df_concat.columns.to_list()
    column_index = [0,5,1,6,2,7,3,8,4,9]
    column_index_order = [re_order[i] for i in column_index]
    df_concat = df_concat[column_index_order]
    df_concat.index = ['CE Side LTP', 'PE Side LTP', 'CE Side Theta', 'PE Side Theta', 'CE Side Vega', 'PE Side Vega', 'CE Side IV', 'PE Side IV', 'CE Side OI', 'PE Side OI', 'CE ATM LTP', 'PE ATM LTP', 'ATM Straddle', 'Spot Price', 'India VIX']

    df_concat.columns = ['1_Initial', '1_Current', '2_Initial', '2_Current', '3_Initial', '3_Current', '4_Initial', '4_Current', '5_Initial', '5_Current']

    den_zero = [(df_concat.iloc[8,1] - df_concat.iloc[8,0]), (df_concat.iloc[9,1] - df_concat.iloc[9,0]), (df_concat.iloc[8,3] - df_concat.iloc[8,2]), (df_concat.iloc[9,3] - df_concat.iloc[9,2]), (df_concat.iloc[8,5] - df_concat.iloc[8,4]), (df_concat.iloc[9,5] - df_concat.iloc[9,4])]

    if all(val != 0 and pd.notna(val) for val in den_zero): # pd.notna(val) is True if val is not NaN &&&&& False is val is NaN
        ab = round((df_concat.iloc[8,1] - df_concat.iloc[8,0]) / (df_concat.iloc[9,1] - df_concat.iloc[9,0]),2)
        ba = round((df_concat.iloc[9,1] - df_concat.iloc[9,0]) / (df_concat.iloc[8,1] - df_concat.iloc[8,0]),2)

        bc = round((df_concat.iloc[8,3] - df_concat.iloc[8,2]) / (df_concat.iloc[9,3] - df_concat.iloc[9,2]),2)
        cb = round((df_concat.iloc[9,3] - df_concat.iloc[9,2]) / (df_concat.iloc[8,3] - df_concat.iloc[8,2]),2)

        cd = round((df_concat.iloc[8,5] - df_concat.iloc[8,4]) / (df_concat.iloc[9,5] - df_concat.iloc[9,4]),2)
        dc = round((df_concat.iloc[9,5] - df_concat.iloc[9,4]) / (df_concat.iloc[8,5] - df_concat.iloc[8,4]),2)

        de = round((df_concat.iloc[8,7] - df_concat.iloc[8,6]) / (df_concat.iloc[9,7] - df_concat.iloc[9,6]),2)
        ed = round((df_concat.iloc[9,7] - df_concat.iloc[9,6]) / (df_concat.iloc[8,7] - df_concat.iloc[8,6]),2)

        ef = round((df_concat.iloc[8,9] - df_concat.iloc[8,8]) / (df_concat.iloc[9,9] - df_concat.iloc[9,8]),2)
        fe = round((df_concat.iloc[9,9] - df_concat.iloc[9,8]) / (df_concat.iloc[8,9] - df_concat.iloc[8,8]),2)

    else:
        ab=ba=bc=cb=cd=dc=de=ed=ef=fe=None

    df_concat['1_Diff'] = [df_concat.iloc[0,1] - df_concat.iloc[0,0], 
                           df_concat.iloc[1,1] - df_concat.iloc[1,0], 
                           df_concat.iloc[2,0] - df_concat.iloc[2,1], 
                           df_concat.iloc[3,0] - df_concat.iloc[3,1], 
                           df_concat.iloc[4,1] - df_concat.iloc[4,0], 
                           df_concat.iloc[5,1] - df_concat.iloc[5,0], 
                           df_concat.iloc[6,1] - df_concat.iloc[6,0], 
                           df_concat.iloc[7,1] - df_concat.iloc[7,0], 
                           f'{df_concat.iloc[8,1] - df_concat.iloc[8,0]}  ({ab})', 
                           f'{df_concat.iloc[9,1] - df_concat.iloc[9,0]}  ({ba})',
                           df_concat.iloc[10,1] - df_concat.iloc[10,0], 
                           df_concat.iloc[11,1] - df_concat.iloc[11,0],
                           df_concat.iloc[12,1] - df_concat.iloc[12,0], 
                           df_concat.iloc[13,1] - df_concat.iloc[13,0],
                           df_concat.iloc[14,1] - df_concat.iloc[14,0]]


    df_concat['2_Diff'] = [df_concat.iloc[0,3] - df_concat.iloc[0,2], 
                           df_concat.iloc[1,3] - df_concat.iloc[1,2], 
                           df_concat.iloc[2,2] - df_concat.iloc[2,3], 
                           df_concat.iloc[3,2] - df_concat.iloc[3,3], 
                           df_concat.iloc[4,3] - df_concat.iloc[4,2], 
                           df_concat.iloc[5,3] - df_concat.iloc[5,2], 
                           df_concat.iloc[6,3] - df_concat.iloc[6,2], 
                           df_concat.iloc[7,3] - df_concat.iloc[7,2], 
                           f'{df_concat.iloc[8,3] - df_concat.iloc[8,2]}  ({bc})',
                           f'{df_concat.iloc[9,3] - df_concat.iloc[9,2]}  ({cb})',
                           df_concat.iloc[10,3] - df_concat.iloc[10,2], 
                           df_concat.iloc[11,3] - df_concat.iloc[11,2],
                           df_concat.iloc[12,3] - df_concat.iloc[12,2], 
                           df_concat.iloc[13,3] - df_concat.iloc[13,2],
                           df_concat.iloc[14,3] - df_concat.iloc[14,2]]

    df_concat['3_Diff'] = [df_concat.iloc[0,5] - df_concat.iloc[0,4], 
                           df_concat.iloc[1,5] - df_concat.iloc[1,4], 
                           df_concat.iloc[2,4] - df_concat.iloc[2,5], 
                           df_concat.iloc[3,4] - df_concat.iloc[3,5], 
                           df_concat.iloc[4,5] - df_concat.iloc[4,4], 
                           df_concat.iloc[5,5] - df_concat.iloc[5,4], 
                           df_concat.iloc[6,5] - df_concat.iloc[6,4], 
                           df_concat.iloc[7,5] - df_concat.iloc[7,4], 
                           f'{df_concat.iloc[8,5] - df_concat.iloc[8,4]}  ({cd})',
                           f'{df_concat.iloc[9,5] - df_concat.iloc[9,4]}  ({dc})',
                           df_concat.iloc[10,5] - df_concat.iloc[10,4], 
                           df_concat.iloc[11,5] - df_concat.iloc[11,4],
                           df_concat.iloc[12,5] - df_concat.iloc[12,4], 
                           df_concat.iloc[13,5] - df_concat.iloc[13,4],
                           df_concat.iloc[14,5] - df_concat.iloc[14,4]]

    df_concat['4_Diff'] = [df_concat.iloc[0,7] - df_concat.iloc[0,6], 
                           df_concat.iloc[1,7] - df_concat.iloc[1,6], 
                           df_concat.iloc[2,6] - df_concat.iloc[2,7], 
                           df_concat.iloc[3,6] - df_concat.iloc[3,7], 
                           df_concat.iloc[4,7] - df_concat.iloc[4,6], 
                           df_concat.iloc[5,7] - df_concat.iloc[5,6], 
                           df_concat.iloc[6,7] - df_concat.iloc[6,6], 
                           df_concat.iloc[7,7] - df_concat.iloc[7,6], 
                           f'{df_concat.iloc[8,7] - df_concat.iloc[8,6]}  ({de})',
                           f'{df_concat.iloc[9,7] - df_concat.iloc[9,6]}  ({ed})',
                           df_concat.iloc[10,7] - df_concat.iloc[10,6], 
                           df_concat.iloc[11,7] - df_concat.iloc[11,6],
                           df_concat.iloc[12,7] - df_concat.iloc[12,6], 
                           df_concat.iloc[13,7] - df_concat.iloc[13,6],
                           df_concat.iloc[14,7] - df_concat.iloc[14,6]]

    df_concat['5_Diff'] = [df_concat.iloc[0,9] - df_concat.iloc[0,8], 
                           df_concat.iloc[1,9] - df_concat.iloc[1,8], 
                           df_concat.iloc[2,8] - df_concat.iloc[2,9], 
                           df_concat.iloc[3,8] - df_concat.iloc[3,9], 
                           df_concat.iloc[4,9] - df_concat.iloc[4,8], 
                           df_concat.iloc[5,9] - df_concat.iloc[5,8], 
                           df_concat.iloc[6,9] - df_concat.iloc[6,8], 
                           df_concat.iloc[7,9] - df_concat.iloc[7,8], 
                           f'{df_concat.iloc[8,9] - df_concat.iloc[8,8]}  ({ef})',
                           f'{df_concat.iloc[9,9] - df_concat.iloc[9,8]}  ({fe})',
                           df_concat.iloc[10,9] - df_concat.iloc[10,8], 
                           df_concat.iloc[11,9] - df_concat.iloc[11,8],
                           df_concat.iloc[12,9] - df_concat.iloc[12,8], 
                           df_concat.iloc[13,9] - df_concat.iloc[13,8],
                           df_concat.iloc[14,9] - df_concat.iloc[14,8]]


    df_concat = df_concat[['1_Initial', '1_Current', '1_Diff', '2_Initial', '2_Current', '2_Diff', '3_Initial', '3_Current', '3_Diff', '4_Initial', '4_Current', '4_Diff', '5_Initial', '5_Current', '5_Diff']]
    df_concat = df_concat.rename(columns={'1_Diff':expiry_name_0, '2_Diff':expiry_name_1, '3_Diff':expiry_name_2, '4_Diff':expiry_name_3, '5_Diff':expiry_name_4})
    return df_concat

# axes = []
# figs = []

# for k in range(0,3):
#     fig, ax = plt.subplots(3,2, figsize=(18,11))
#     fig.subplots_adjust(left=0.03, right=0.99, bottom=0.035, top=0.95, wspace=0.1, hspace=0.075)
#     axes.append(ax)
#     figs.append(fig)

def plot(graph_data):
    global axes, figs
    plt.ion()

    df = pd.DataFrame(graph_data)

    for i in range(0,3):
        df.iloc[0,i] = pd.Series(df.iloc[0,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[1,i] = pd.Series(df.iloc[1,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[2,i] = pd.Series(df.iloc[2,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[3,i] = pd.Series(df.iloc[3,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[4,i] = pd.Series(df.iloc[4,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[5,i] = pd.Series(df.iloc[5,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[6,i] = pd.Series(df.iloc[6,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[7,i] = pd.Series(df.iloc[7,i]).ewm(span=300, adjust=False).mean().tolist()
        df.iloc[8,i] = pd.Series(df.iloc[8,i]).ewm(span=100, adjust=False).mean().tolist()
        df.iloc[9,i] = pd.Series(df.iloc[9,i]).ewm(span=100, adjust=False).mean().tolist()
        df.iloc[10,i] = pd.Series(df.iloc[10,i]).ewm(span=100, adjust=False).mean().tolist()
        df.iloc[11,i] = pd.Series(df.iloc[11,i]).ewm(span=100, adjust=False).mean().tolist()
        df.iloc[12,i] = pd.Series(df.iloc[12,i]).ewm(span=100, adjust=False).mean().tolist()
        df.iloc[13,i] = pd.Series(df.iloc[13,i]).ewm(span=300, adjust=False).mean().tolist()

        # fig, ax = plt.subplots(3,2, figsize=(18,11))
        # fig.subplots_adjust(left=0.03, right=0.99, bottom=0.035, top=0.95, wspace=0.1, hspace=0.075)

        ax = axes[i]
        fig = figs[i]

        for j in range(0,2):
            ax[0, j].cla()  # Clear previous plots in this subplot
            ax[1, j].cla()
            ax[2, j].cla()

        fig.suptitle(f'{df.columns[i]}', fontsize=16)

        # First Column
        ax[0,0].plot(df.iloc[0,i], label=df.index[0])
        ax[0,0].plot(df.iloc[1,i], label=df.index[1])
        ax[0,0].legend(fontsize=15)

        ax[1,0].plot(df.iloc[2,i], label=df.index[2])
        ax[1,0].plot(df.iloc[3,i], label=df.index[3])
        ax[1,0].legend(fontsize=15)

        ax[2,0].plot(df.iloc[4,i], label=df.index[4])
        ax[2,0].plot(df.iloc[5,i], label=df.index[5])
        ax[2,0].legend(fontsize=15)

        # Second Column
        ax[0,1].plot(df.iloc[6,i], label=df.index[6])
        ax[0,1].plot(df.iloc[7,i], label=df.index[7])
        ax[0,1].legend(fontsize=15)

        ax[1,1].plot(df.iloc[8,i], label=df.index[8])
        ax[1,1].plot(df.iloc[9,i], label=df.index[9])
        ax[1,1].legend(fontsize=15)

        ax[2,1].plot(df.iloc[10,i], label=df.index[10])
        ax[2,1].plot(df.iloc[11,i], label=df.index[11])
        ax[2,1].plot(df.iloc[12,i], label=df.index[12])
        ax[2,1].legend(fontsize=15)

        plt.pause(1)   

    plt.ioff()

counter = 1
last_triggered_minute = None

def chain(instrument_key,expiry_date,counter):

        global structure_initial, structure_current, past_data, initialize
        
        url1 = 'https://api.upstox.com/v2/option/chain'
        url2 = 'https://api.upstox.com/v2/market-quote/ltp?instrument_key=NSE_INDEX|India VIX'

        params = {
                'instrument_key': instrument_key,
                'expiry_date': expiry_date
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access}'
        }

        response = requests.get(url1, params=params, headers=headers)
        time.sleep(1)

        response2 = requests.get(url2, headers=headers)
        response2 = response2.json()
        india_vix = response2['data']['NSE_INDEX:India VIX']['last_price']

        time_stamp = datetime.now().strftime("%H:%M:%S")
        option = response.json()
        option_df = pd.json_normalize(option['data'])

        option_df = option_df[['expiry', 'pcr', 'strike_price', 'underlying_spot_price', 'call_options.instrument_key', 'call_options.market_data.ltp', 'call_options.market_data.oi', 'call_options.option_greeks.vega', 'call_options.option_greeks.theta', 'call_options.option_greeks.gamma', 'call_options.option_greeks.delta', 'call_options.option_greeks.iv', 'put_options.instrument_key', 'put_options.market_data.ltp', 'put_options.market_data.oi', 'put_options.option_greeks.vega', 'put_options.option_greeks.theta', 'put_options.option_greeks.gamma', 'put_options.option_greeks.delta', 'put_options.option_greeks.iv']]
        option_df = option_df.rename(columns={'call_options.instrument_key' : 'CE_instrument_key', 'call_options.market_data.ltp' : 'CE_ltp', 'call_options.market_data.oi' : 'CE_oi', 'call_options.option_greeks.vega' : 'CE_vega', 'call_options.option_greeks.theta' : 'CE_theta', 'call_options.option_greeks.gamma' : 'CE_gamma', 'call_options.option_greeks.delta' : 'CE_delta', 'call_options.option_greeks.iv' : 'CE_iv', 'put_options.instrument_key' : 'PE_instrument_key', 'put_options.market_data.ltp' : 'PE_ltp', 'put_options.market_data.oi' : 'PE_oi', 'put_options.option_greeks.vega' : 'PE_vega', 'put_options.option_greeks.theta' : 'PE_theta', 'put_options.option_greeks.gamma' : 'PE_gamma', 'put_options.option_greeks.delta' : 'PE_delta', 'put_options.option_greeks.iv' : 'PE_iv', 'underlying_spot_price' : 'spot_price'})
        option_df = option_df[['expiry','pcr','CE_instrument_key','CE_delta','CE_oi','CE_iv','CE_vega','CE_theta','CE_ltp','strike_price','PE_ltp','PE_theta','PE_vega','PE_iv','PE_oi','PE_delta','PE_instrument_key','spot_price']]

        option_df['diff'] = abs(option_df['spot_price'] - option_df['strike_price'])
        ce = option_df.loc[option_df['diff'].idxmin(),'CE_ltp']
        strike = option_df.loc[option_df['diff'].idxmin(),'strike_price']
        pe = option_df.loc[option_df['diff'].idxmin(),'PE_ltp']

        fut_spot_price = ce-pe+strike

        option_df['spot_price'] = fut_spot_price
        option_df['diff'] = abs(option_df['spot_price'] - option_df['strike_price'])
        option_df['prem_diff'] = option_df['CE_ltp'] - option_df['PE_ltp']
        option_df['CE/PE'] = round((option_df['CE_ltp'] / option_df['PE_ltp']),2)
        atm_strike = option_df.loc[option_df['diff'].idxmin(), 'strike_price']

        ce_atm_ltp = option_df[option_df['strike_price'] == atm_strike].iloc[0]['CE_ltp']
        pe_atm_ltp = option_df[option_df['strike_price'] == atm_strike].iloc[0]['PE_ltp']

        x = option_df['strike_price'].diff().mode()[0]
        upper_limit = atm_strike + 15*x
        lower_limit = atm_strike - 15*x
        option_df = option_df[(option_df['strike_price'] >= lower_limit) & (option_df['strike_price'] <= upper_limit)]

        ce_df = option_df[option_df['strike_price'] >= atm_strike]
        pe_df = option_df[option_df['strike_price'] <= atm_strike]

        ce_ltp_sum = round(ce_df['CE_ltp'].sum(),2)
        pe_ltp_sum = round(pe_df['PE_ltp'].sum(),2)
        ce_theta_sum = round(ce_df['CE_theta'].sum(),2)
        pe_theta_sum = round(pe_df['PE_theta'].sum(),2)
        ce_vega_sum = round(ce_df['CE_vega'].sum(),2)
        pe_vega_sum = round(pe_df['PE_vega'].sum(),2)
        ce_iv_sum = round(ce_df['CE_iv'].sum(),2)
        pe_iv_sum = round(pe_df['PE_iv'].sum(),2)
        ce_oi_sum = round(ce_df['CE_oi'].sum(),2)
        pe_oi_sum = round(pe_df['PE_oi'].sum(),2)

        try:
            with open('initial_values.json', 'r') as file_read:
                structure_initial = json.load(file_read)
        except:
            if counter<=3:

                structure_initial[f'{instrument_key}_{expiry_date}_initial'] = {'ce_ltp_init' : ce_ltp_sum,
                                                                                'pe_ltp_init' : pe_ltp_sum,
                                                                                'ce_theta_init' : ce_theta_sum,
                                                                                'pe_theta_init' : pe_theta_sum,
                                                                                'ce_vega_init' : ce_vega_sum,
                                                                                'pe_vega_init' : pe_vega_sum,
                                                                                'ce_iv_init' : ce_iv_sum,
                                                                                'pe_iv_init' : pe_iv_sum,
                                                                                'ce_oi_init' : ce_oi_sum,
                                                                                'pe_oi_init' : pe_oi_sum,
                                                                                'ce_atm_ltp' : ce_atm_ltp,
                                                                                'pe_atm_ltp' : pe_atm_ltp,
                                                                                'atm_straddle' : (ce_atm_ltp + pe_atm_ltp),
                                                                                'spot price' : fut_spot_price,
                                                                                'india vix' : india_vix
                                                                                }

        structure_current[f'{instrument_key}_{expiry_date}_Current'] = {'ce_ltp_current' : ce_ltp_sum,
                                                                        'pe_ltp_current' : pe_ltp_sum,
                                                                        'ce_theta_current' : ce_theta_sum,
                                                                        'pe_theta_current' : pe_theta_sum,
                                                                        'ce_vega_current' : ce_vega_sum,
                                                                        'pe_vega_current' : pe_vega_sum,
                                                                        'ce_iv_current' : ce_iv_sum,
                                                                        'pe_iv_current' : pe_iv_sum,
                                                                        'ce_oi_current' : ce_oi_sum,
                                                                        'pe_oi_current' : pe_oi_sum,
                                                                        'ce_atm_ltp' : ce_atm_ltp,
                                                                        'pe_atm_ltp' : pe_atm_ltp,
                                                                        'atm_straddle' : (ce_atm_ltp + pe_atm_ltp),
                                                                        'spot price' : fut_spot_price,
                                                                        'india vix' : india_vix
                                                                        }

        ce_ltp_diff = round((ce_ltp_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_ltp_init']),2)
        pe_ltp_diff = round((pe_ltp_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_ltp_init']),2)
        ce_theta_diff = round((structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_theta_init'] - ce_theta_sum),2)
        pe_theta_diff = round((structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_theta_init'] - pe_theta_sum),2)
        ce_vega_diff = round((ce_vega_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_vega_init']),2)
        pe_vega_diff = round((pe_vega_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_vega_init']),2)
        ce_iv_diff = round((ce_iv_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_iv_init']),2)
        pe_iv_diff = round((pe_iv_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_iv_init']),2)
        ce_oi_diff = round((ce_oi_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_oi_init']),2)
        pe_oi_diff = round((pe_oi_sum - structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_oi_init']),2)
        ce_atm_diff = round((ce_atm_ltp - structure_initial[f'{instrument_key}_{expiry_date}_initial']['ce_atm_ltp']),2)
        pe_atm_diff = round((pe_atm_ltp - structure_initial[f'{instrument_key}_{expiry_date}_initial']['pe_atm_ltp']),2)
        atm_straddle_diff = round(((ce_atm_ltp + pe_atm_ltp) - structure_initial[f'{instrument_key}_{expiry_date}_initial']['atm_straddle']),2)
        spot_price_diff = round((fut_spot_price - structure_initial[f'{instrument_key}_{expiry_date}_initial']['spot price']),2)
        india_vix_diff = round((india_vix - structure_initial[f'{instrument_key}_{expiry_date}_initial']['india vix']),2)

        main = {'CE Side LTP':ce_ltp_diff, 'PE Side LTP':pe_ltp_diff, 'CE Side Theta':ce_theta_diff, 'PE Side Theta':pe_theta_diff, 'CE Side Vega':ce_vega_diff, 'PE Side Vega':pe_vega_diff, 'CE Side IV':ce_iv_diff, 'PE Side IV':pe_iv_diff, 'CE Side OI':ce_oi_diff, 'PE Side OI':pe_oi_diff, 'CE ATM LTP':ce_atm_diff, 'PE ATM LTP':pe_atm_diff, 'Atm Straddle':atm_straddle_diff, 'Spot Price': spot_price_diff, 'India Vix': india_vix_diff, 'Time': time_stamp}

        expiry_name = option_df.iloc[0,0]

        main_df = pd.DataFrame([main], index=[expiry_name]).T

        try:
            if expiry_date == expiry_list_nifty[0]:
                with open('past_data.json', 'r') as file_read:
                    past_data = json.load(file_read)
                    initialize=2
        except:
            pass

        if initialize==1:
            past_data[f'{instrument_key}_{expiry_date}'] = {'ce_ltp': [], 'pe_ltp': [], 'ce_theta' : [], 'pe_theta' : [], 'ce_vega' : [], 'pe_vega' : [], 'ce_iv' : [], 'pe_iv' : [], 'ce_oi' : [], 'pe_oi' : [], 'ce_atm' : [], 'pe_atm' : [], 'atm_straddle' : [], 'spot_price':[], 'india_vix':[], 'time' : []}
       
        past_data[f'{instrument_key}_{expiry_date}']['ce_ltp'].append(main_df.iloc[0,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_ltp'].append(main_df.iloc[1,0])
        past_data[f'{instrument_key}_{expiry_date}']['ce_theta'].append(main_df.iloc[2,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_theta'].append(main_df.iloc[3,0])
        past_data[f'{instrument_key}_{expiry_date}']['ce_vega'].append(main_df.iloc[4,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_vega'].append(main_df.iloc[5,0])
        past_data[f'{instrument_key}_{expiry_date}']['ce_iv'].append(main_df.iloc[6,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_iv'].append(main_df.iloc[7,0])
        past_data[f'{instrument_key}_{expiry_date}']['ce_oi'].append(main_df.iloc[8,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_oi'].append(main_df.iloc[9,0])
        past_data[f'{instrument_key}_{expiry_date}']['ce_atm'].append(main_df.iloc[10,0])
        past_data[f'{instrument_key}_{expiry_date}']['pe_atm'].append(main_df.iloc[11,0])
        past_data[f'{instrument_key}_{expiry_date}']['atm_straddle'].append(main_df.iloc[12,0])
        past_data[f'{instrument_key}_{expiry_date}']['spot_price'].append(main_df.iloc[13,0])
        # past_data[f'{instrument_key}_{expiry_date}']['india_vix'].append(main_df.iloc[14,0])
        past_data[f'{instrument_key}_{expiry_date}']['india_vix'].append(india_vix)
        past_data[f'{instrument_key}_{expiry_date}']['time'].append(main_df.iloc[15,0])
        
        return option_df, main_df, expiry_name

while True:

    # tt1 = time.time()
    nifty_0_chain, nifty_0_main_df, expiry_name_0 = chain(instrument_key_nifty,expiry_list_nifty[0],a)
    nifty_1_chain, nifty_1_main_df, expiry_name_1 = chain(instrument_key_nifty,expiry_list_nifty[1],b)
    nifty_3_chain, nifty_3_main_df, expiry_name_2 = chain(instrument_key_nifty,expiry_list_nifty[2],c)
    bnf_0_chain, bnf_0_main_df, expiry_name_3 = chain(instrument_key_bnf,expiry_list_bnf[0],d)
    sensex_0_chain, sensex_0_main_df, expiry_name_4 = chain(instrument_key_sensex,expiry_list_sensex[0],e)
    initialize=2
    # breakpoint()
    df_concat = check_data(structure_initial,structure_current)

    if a==b==c==d==e==3:
        with open('initial_values.json', 'w') as file_write:
            json.dump(structure_initial, file_write)

    with open('past_data.json', 'w') as file_write:
        json.dump(past_data, file_write)

    # now = datetime.now()
    # minute = now.minute
    # second = now.second

    # if minute % 5 == 0 and second == 0 and minute != last_triggered_minute:
    #     plot(past_data)
    #     last_triggered_minute = minute
    
    summary.range('C2').value = nifty_0_main_df
    summary.range('F2').value = nifty_1_main_df
    summary.range('I2').value = nifty_3_main_df
    summary.range('L2').value = bnf_0_main_df
    summary.range('O2').value = sensex_0_main_df

    summary.range('A20').value = df_concat

    # nifty_0.range('A1').value = nifty_0_chain
    # nifty_1.range('A1').value = nifty_1_chain
    # nifty_3.range('A1').value = nifty_3_chain
    # bnf_0.range('A1').value = bnf_0_chain
    # sensex_0.range('A1').value = sensex_0_chain
    
    if a<=3:
        a=a+1
        b=b+1
        c=c+1
        d=d+1
        e=e+1
    # tt2 = time.time()
    # print(f'{counter}, {tt2-tt1}')
    # counter += 1