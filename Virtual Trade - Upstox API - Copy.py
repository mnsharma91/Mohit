import requests
import pandas as pd
import json
import xlwings as xw
import time
from datetime import datetime
import openpyxl
import matplotlib.pyplot as plt
from pprint import pprint
import asyncio
import ssl
import websockets
from google.protobuf.json_format import MessageToDict
from threading import Thread
import threading
import numpy as np
import MarketDataFeedV3_pb2 as pb

live_data = {}
dict_lock = threading.Lock()
excel_lock = threading.Lock()

##############################################################################

try:
    with open('access_code.json', 'r') as file_read:
        access = json.load(file_read)
except:
    api_key = '061ca76c-53f6-4d77-8f1a-01a3e34b3a01'
    api_secret = 'wurhexh7lo'
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

    with open('access_code.json', 'w') as file_write:
        json.dump(access, file_write)

########################################################################################

# with open('final_list.json', 'r') as file_read:
#     final_list = json.load(file_read)

def get_market_data_feed_authorize_v3():
    """Get authorization for market data feed."""
    global access
    access_token = access
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
    api_response = requests.get(url=url, headers=headers)
    return api_response.json()


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = pb.FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


async def fetch_market_data():
    """Fetch market data using WebSocket and print it."""
    global live_data, final_list
    # Create default SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Get market data feed authorization
    response = get_market_data_feed_authorize_v3()
    # Connect to the WebSocket with SSL context
    async with websockets.connect(response["data"]["authorized_redirect_uri"], ssl=ssl_context) as websocket:
        print('Connection established')

        await asyncio.sleep(1)  # Wait for 1 second

        # Data to be sent over the WebSocket
        data = {
            "guid": "someguid",
            "method": "sub",
            "data": {
                "mode": "full",
                # "instrumentKeys": ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"]
                "instrumentKeys": final_list
            }
        }

        # Convert data to binary and send over WebSocket
        binary_data = json.dumps(data).encode('utf-8')
        await websocket.send(binary_data)

        # Continuously receive and decode data from WebSocket
        while True:
            message = await websocket.recv()
            decoded_data = decode_protobuf(message)

            # Convert the decoded data to a dictionary
            data_dict = MessageToDict(decoded_data)

            if 'feeds' in data_dict:
                data = data_dict['feeds']
                for key, value in data.items():

                    ltp = value.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {}).get('ltp') # {'ltp' : value['fullFeed']['marketFF']['ltpc']['ltp']
                    delta = value.get('fullFeed', {}).get('marketFF', {}).get('optionGreeks', {}).get('delta') # {'delta' : value['fullFeed']['marketFF']['optionGreeks']['delta']}

                    with dict_lock:
	                    if key not in live_data:
	                        live_data[key] = {}

	                    if ltp is not None:
	                        live_data[key]['ltp'] = ltp

	                    if delta is not None:
	                        live_data[key]['delta'] = delta

            # print(live_data)


########################################################################################
def PNL(df,spot):

	first_index_set = False

	df['strike'] = df['symbol'].astype(str).str[0:5].astype(int)
	df['type'] = df['symbol'].astype(str).str[-2:]
	df = df.rename(columns={'signal':'action', 'ltp_entry':'premium'})
	df = df[['type', 'action', 'strike', 'premium']]

	spot_price = np.arange(spot*0.95, spot*1.05, 1)
	total_payoff = np.zeros_like(spot_price, dtype=float)

	for index, leg in df.iterrows():
	    if leg['type'] == 'CE':
	        intrensic = np.maximum(spot_price - leg['strike'], 0)
	    elif leg['type'] == 'PE':
	        intrensic = np.maximum(leg['strike'] - spot_price, 0)

	    payoff = (intrensic - leg['premium'])
	    if leg['action'] == 'S':
	        payoff = -payoff

	    total_payoff = total_payoff + payoff

	max_profit = round(max(total_payoff),2)
	max_loss = round(min(total_payoff),2)

	if  (total_payoff[0] != total_payoff[1]) or (total_payoff[-1] != total_payoff[-2]):
	    if (total_payoff[0] != total_payoff[1]):
	        if total_payoff[0] < 0:
	            max_loss = 'Unlimited'
	        elif total_payoff[0] > 0:
	            max_profit = 'Unlimited'

	    elif (total_payoff[-1] != total_payoff[-2]):
	        if total_payoff[-1] < 0:
	            max_loss = 'Unlimited'
	        elif total_payoff[-1] > 0:
	            max_profit = 'Unlimited'

	sign_rev = np.sign(total_payoff)

	for i in range(1,len(sign_rev)):
	    if (sign_rev[i] != sign_rev[i-1]) and first_index_set == False:
	        low_index = i
	        first_index_set = True

	    elif (sign_rev[i] != sign_rev[i-1]) and first_index_set == True:
	        high_index = i

	breakeven_low = int(spot_price[low_index])
	try:
		breakeven_high = int(spot_price[high_index])
	except:
		breakeven_high = None

	return max_profit, max_loss, breakeven_low, breakeven_high



def get_time():
	x = datetime.now()
	time = x.strftime("%d-%m-%Y / %I:%M %p")
	return time


def instrument():
    inst_url = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    instrument = pd.read_csv(inst_url)
    instrument.to_csv('instrument.csv')


def update_subscription_list():
	global expiry_list_nifty, expiry_list_bnf, expiry_list_sensex
	instrument_key_nifty = 'NSE_INDEX|Nifty 50'
	instrument_key_bnf = 'NSE_INDEX|Nifty Bank'
	instrument_key_sensex = 'BSE_INDEX|SENSEX'

	nifty_0_list = option_chain(instrument_key_nifty,expiry_list_nifty[0],ocs=0)
	nifty_1_list = option_chain(instrument_key_nifty,expiry_list_nifty[1],ocs=0)
	nifty_2_list = option_chain(instrument_key_nifty,expiry_list_nifty[2],ocs=0)
	nifty_3_list = option_chain(instrument_key_nifty,expiry_list_nifty[3],ocs=0)
	bnf_0_list = option_chain(instrument_key_bnf,expiry_list_bnf[0],ocs=0)
	sensex_0_list = option_chain(instrument_key_sensex,expiry_list_sensex[0],ocs=0)

	final_list = nifty_0_list + nifty_1_list + nifty_2_list + nifty_3_list + bnf_0_list + sensex_0_list
	return final_list


def option_chain(instrument_key,expiry_date,ocs):
    global access
    url = 'https://api.upstox.com/v2/option/chain'
    params = {
            'instrument_key': instrument_key,
            'expiry_date': expiry_date
    }
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access}'
    }

    response = requests.get(url, params=params, headers=headers)
    time.sleep(1)
    time_stamp = datetime.now().strftime("%H:%M:%S")
    option = response.json()
    option_df = pd.json_normalize(option['data'])
    option_df = option_df[['expiry', 'strike_price', 'underlying_spot_price', 'call_options.instrument_key', 'call_options.market_data.ltp',  'put_options.instrument_key', 'put_options.market_data.ltp', ]]
    option_df = option_df.rename(columns={'call_options.instrument_key' : 'CE_instrument_key', 'call_options.market_data.ltp' : 'CE_ltp', 'put_options.instrument_key' : 'PE_instrument_key', 'put_options.market_data.ltp' : 'PE_ltp', 'underlying_spot_price' : 'spot_price'})
    option_df[['signal_ce', 'signal_pe']] = None

    if instrument_key == 'NSE_INDEX|Nifty 50':
    	option_df[['lotsize', 'Index']] = [75, 'Nifty 50']
    elif instrument_key == 'NSE_INDEX|Nifty Bank':
    	option_df[['lotsize', 'Index']] = [30, 'Bank Nifty']
    else:
    	option_df[['lotsize', 'Index']] = [20, 'Sensex']

    option_df['symbol_ce'] = option_df['strike_price'].astype(str) + '_CE'
    option_df['symbol_pe'] = option_df['strike_price'].astype(str) + '_PE'
    
    option_df = option_df[['Index','expiry','lotsize','CE_instrument_key' ,'symbol_ce','CE_ltp','signal_ce','strike_price','signal_pe','PE_ltp','symbol_pe','PE_instrument_key','spot_price']]

    option_df['diff'] = abs(option_df['spot_price'] - option_df['strike_price'])
    ce = option_df.loc[option_df['diff'].idxmin(),'CE_ltp']
    strike = option_df.loc[option_df['diff'].idxmin(),'strike_price']
    pe = option_df.loc[option_df['diff'].idxmin(),'PE_ltp']

    fut_spot_price = ce-pe+strike

    option_df['spot_price'] = fut_spot_price
    option_df['diff'] = abs(option_df['spot_price'] - option_df['strike_price'])
    # option_df['prem_diff'] = option_df['CE_ltp'] - option_df['PE_ltp']
    # option_df['CE/PE'] = round((option_df['CE_ltp'] / option_df['PE_ltp']),2)
    atm_strike = option_df.loc[option_df['diff'].idxmin(), 'strike_price']

    ce_atm_ltp = option_df[option_df['strike_price'] == atm_strike].iloc[0]['CE_ltp']
    pe_atm_ltp = option_df[option_df['strike_price'] == atm_strike].iloc[0]['PE_ltp']

    x = option_df['strike_price'].diff().mode()[0]
    upper_limit = atm_strike + 15*x
    lower_limit = atm_strike - 15*x
    option_df = option_df[(option_df['strike_price'] >= lower_limit) & (option_df['strike_price'] <= upper_limit)]

    if ocs == 0:
	    list1 = option_df['CE_instrument_key'].tolist()
	    list2 = option_df['PE_instrument_key'].tolist()
	    t_list = list1 + list2
	    return t_list

    if ocs >= 1:
	    wb = xw.Book(f'merge.xlsx')
	    option_chain = wb.sheets(f'option_chain_{ocs}')
	    option_chain.clear_contents()
	    option_chain.range('A1').value = option_df

###############################################################################################################


def position(lot,ocs,cell):
	
	first_run = True
	main = {}
	m=1
	current = {}
	condition = 1
	# wb = xw.Book('merge.xlsx')
	with excel_lock:
		wb = xw.Book(f'merge.xlsx')
		option_chain = wb.sheets(f'option_chain_{ocs}')
		trade = wb.sheets(f'trade_{ocs}')
		trade.clear_contents()

	while condition:
		z=0
		y=0
		structure={}
		with excel_lock:
			try:
				data = option_chain.range('A2:O32').value
			except Exception as e:
				print(f'[Read Skipped] Reason: {e}')
				print(f"Thread Disturbed during reading Data from Option's Sheet, Reading Skipped...")

		for x in data:
			sr, index, expiry, lotsize, token_ce, symbol_ce, ce_ltp, signal_ce, strike, signal_pe, pe_ltp, symbol_pe, token_pe, spot, diff = x
			if signal_ce or signal_pe :
				try:
					if signal_ce:
						
						if (signal_ce == 'B') or (signal_ce == 'BC'):
							with dict_lock:
								ltp = live_data[token_ce]['ltp'] if signal_ce == 'B' else current[str(y)]['ltp']
								delta = live_data[token_ce]['delta'] if signal_ce == 'B' else current[str(y)]['delta']
							current[str(y)] = {'ltp':ltp, 'delta':delta}
							exit_time = None if signal_ce == 'B' else get_time()
							structure[str(y)] = {'entry time':get_time(), 'exit time': exit_time, 'expiry':expiry, 'token':token_ce, 'index':index, 'symbol':symbol_ce, 'signal':signal_ce, 'delta':delta, 'qty':lotsize, 'lot': None, 'ltp_entry': ltp}
							y=y+1
							# time.sleep(0.1)

						elif (signal_ce == 'S') or (signal_ce == 'SC'):
							with dict_lock:
								ltp = live_data[token_ce]['ltp'] if signal_ce == 'S' else current[str(y)]['ltp']
								delta = live_data[token_ce]['delta'] if signal_ce == 'S' else current[str(y)]['delta']
							current[str(y)] = {'ltp':ltp, 'delta':delta}
							exit_time = None if signal_ce == 'S' else get_time()
							structure[str(y)] = {'entry time':get_time(), 'exit time': exit_time, 'expiry':expiry, 'token':token_ce, 'index':index, 'symbol':symbol_ce, 'signal':signal_ce, 'delta':delta, 'qty':lotsize, 'lot': None, 'ltp_entry': ltp}
							y=y+1
							# time.sleep(0.1)

					if signal_pe:

						if (signal_pe == 'B') or (signal_pe == 'BC'):
							with dict_lock:
								ltp = live_data[token_pe]['ltp'] if signal_pe == 'B' else current[str(y)]['ltp']
								delta = live_data[token_pe]['delta'] if signal_pe == 'B' else current[str(y)]['delta']
							current[str(y)] = {'ltp':ltp, 'delta':delta}
							exit_time = None if signal_pe == 'B' else get_time()
							structure[str(y)] = {'entry time':get_time(), 'exit time': exit_time, 'expiry':expiry, 'token':token_pe, 'index':index, 'symbol':symbol_pe, 'signal':signal_pe, 'delta':delta, 'qty':lotsize, 'lot': None, 'ltp_entry': ltp}
							y=y+1
							# time.sleep(0.1)

						elif (signal_pe == 'S') or (signal_pe == 'SC'):
							with dict_lock:
								ltp = live_data[token_pe]['ltp'] if signal_pe == 'S' else current[str(y)]['ltp']
								delta = live_data[token_pe]['delta'] if signal_pe == 'S' else current[str(y)]['delta']
							current[str(y)] = {'ltp':ltp, 'delta':delta}
							exit_time = None if signal_pe == 'S' else get_time()
							structure[str(y)] = {'entry time':get_time(), 'exit time': exit_time, 'expiry':expiry, 'token':token_pe, 'index':index, 'symbol':symbol_pe, 'signal':signal_pe, 'delta':delta, 'qty':lotsize, 'lot': None, 'ltp_entry': ltp}
							y=y+1
							# time.sleep(0.1)
				except Exception as e:
					print(f"[ERROR] ltpData failed for {symbol_ce}: {e}")
					continue 


		if m==1 :
			main = structure.copy()
			m=m+1

		for i in range(len(main)):
			try:
				if str(i) not in structure:
				    print(f"[WARN] Structure key {i} completely missing, skipping update.")
				    continue  # Skip this iteration
				else:
				    main[str(i)]['ltp_current'] = structure[str(i)]['ltp_entry']
				    main[str(i)]['delta'] = structure[str(i)]['delta']

				    if main[str(i)]['exit time'] is None:
				        main[str(i)]['exit time'] = structure[str(i)]['exit time']
			except KeyError:
				print(f"[WARN] Missing key {i} in structure. Possibly due to timeout or API failure.")
				main[str(i)]['ltp_current'] = main[str(i)]['ltp_entry']  # fallback to original
				# You can keep exit time unchanged or set a default

		df = pd.DataFrame(main).T

		df['lot'] = lot
		if first_run:
			max_profit, max_loss, bke_l, bke_h = PNL(df.copy(),spot)
			first_run = False

		df['Points'] = np.where(df['signal'] == 'B', (df['ltp_current'] - df['ltp_entry']), (df['ltp_entry'] - df['ltp_current']))
		df['P&L'] = np.where(df['signal'] == 'B', (df['ltp_current'] - df['ltp_entry'])*df['qty']*df['lot'], (df['ltp_entry'] - df['ltp_current'])*df['qty']*df['lot'])
		
		# breakpoint()
		df.loc[len(df)] = [None, None, None, None, None, None, None, df['delta'].sum(), None, None, None, df['Points'].sum(), 'Net P&L', df['P&L'].sum()]
		df.loc[len(df)] = [None, None, 'Max Profit', max_profit if max_profit=='Unlimited' else max_profit*lotsize*lot, None, None, None, None, None, None, None, None, None, None]
		df.loc[len(df)] = [None, None, 'Max Loss', max_loss if max_loss=='Unlimited' else max_loss*lotsize*lot, None, None, None, None, None, None, None, None, None, None]
		df.loc[len(df)] = [None, None, 'Breakeven', f'{bke_l} : {bke_h}', None, None, None, None, None, None, None, None, None, None]

		with excel_lock:
			trade.range(f'A{cell}').value = df


		for i in range(len(main)):
			if main[str(i)]['exit time'] != None:
				z=z+1
				if z==y:
					condition=0

##############################################################################################################


def run_websocket():
    """Run WebSocket in a background thread."""
    asyncio.run(fetch_market_data())


ref_inst = input('Do you want to refresh Instrument Data : 1 / 0 : ')

if ref_inst == '1' :
	instrument()
	print('Instrument Data Updated')
else:
	pass

df = pd.read_csv('instrument.csv')

df_niftyoptions = df[(df['exchange'] == 'NSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'NIFTY')]
expiry_list_nifty = df_niftyoptions['expiry'].unique().tolist()
expiry_list_nifty.sort()

df_bnf = df[(df['exchange'] == 'NSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'BANKNIFTY')]
expiry_list_bnf = df_bnf['expiry'].unique().tolist()
expiry_list_bnf.sort()

df_sensex = df[(df['exchange'] == 'BSE_FO') & (df['instrument_type'] == 'OPTIDX') & (df['name'] == 'SENSEX')]
expiry_list_sensex = df_sensex['expiry'].unique().tolist()
expiry_list_sensex.sort()

# final_list = update_subscription_list()


sub_list = input('Do you want to Update Subscription List : 1 / 0 : ')

if sub_list == '1':
	final_list = update_subscription_list()
	print('Websocket Subscription List Updated')
	with open('final_list.json', 'w') as file_write:
		json.dump(final_list, file_write)
else:
	try:
		with open('final_list.json', 'r') as file_read:
			final_list = json.load(file_read)
	except:
		final_list = update_subscription_list()
		with open('final_list.json', 'w') as file_write:
			json.dump(final_list, file_write)
			print('Subscription List File Not Found, but now Created & Updated')


# Start WebSocket in background
threading.Thread(target=run_websocket, daemon=True).start()


# Wait for live_data to populate
while not live_data:
    print("Waiting for live data to populate...")
    time.sleep(1)

ocs=1
first_trade=1
opt = 1

while True :

	index_no = input("Enter Index : 1:Nifty / 2:Bank-Nifty / 3:Sensex : ")
	index = 'NSE_INDEX|Nifty 50' if index_no == '1' else 'NSE_INDEX|Nifty Bank' if index_no == '2' else 'BSE_INDEX|SENSEX'

	if index_no == '1':
		expiry = int(input("Enter Nifty Expiry No. : 0 / 1 / 2 / 3 : "))
		expiry = expiry_list_nifty[expiry]

	elif index_no == '2':
		expiry = int(input("Enter Bank-Nifty Expiry No. : 0 : "))
		expiry = expiry_list_bnf[expiry]

	else:
		expiry = int(input("Enter Sensex Expiry No. : 0 : "))
		expiry = expiry_list_sensex[expiry]


	option_chain(index,expiry,ocs)

	lot = int(input('Enter the Lot : '))
	input('Select the Strikes in Excel and Press Enter to Continue... ')

	# wb = xw.Book('merge.xlsx')
	# trade = wb.sheets('trade')
	# trade.clear_contents()

	if first_trade==1:
		t1 = Thread(target=position, args=(lot,ocs,1))
		t1.start()
		first_trade=0
		ocs = ocs+1
	else :
		t2 = Thread(target=position, args=(lot,ocs,1))
		t2.start()

	if opt == 1:
		next_trade = input('Do you want to place 2nd Trade : Y / N : ')
		if next_trade=='Y':
			opt=2
			continue
		else:
			break
	else:
		break
