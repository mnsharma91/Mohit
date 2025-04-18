from threading import Thread, Lock
import xlwings as xw
import time
import pythoncom

lock = Lock()
wb = xw.Book('excel.xlsx')
sht1 = wb.sheets('Sheet1')
sht2 = wb.sheets('Sheet2')
sht3 = wb.sheets('Sheet3')

def func():
	pythoncom.CoInitialize()
	while True:
		with lock:
			sht1.range('A1:A10').value = 25
			sht2.range('A1:A10').value = 50
			x = sht1.range('A1:A10').value
			y = sht2.range('A1:A10').value
			sht3.range('A10:A20').value = x
			sht3.range('A1:A10').value = y
			time.sleep(0.1)
	pythoncom.CoUninitialize()

t1 = Thread(target=func)
t2 = Thread(target=func)

t1.start()
t2.start()







