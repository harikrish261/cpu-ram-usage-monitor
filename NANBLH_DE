import os
import sys
import extract_msg
import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial
from sql

def extract_attachements(out_dir, msg_file):
	'''This function will process a message file and save the valid attachments
	to the given output directory
	out_dir: string, path output directory
	msg_file: string, path to the message file'''
	i = 0
	msg = extract_msg.Message(msg_file)
	entity_sub = msg.subject.upper()
	entity_sub = entity_sub.repalce("FW: ", "")
	entity_sub = entity_sub.repalce("RE: ", "")
	entity_sub = entity_sub.repalce("NATIONAL", "") # Known issue if entity name has National
	entity_sub = entity_sub.repalce("ACCOUNT", "")	# Known issue if entity name has Account
	for attachment in msg.attachments:
		if (".xls" in attachment.longFilename):
			attachment.save(customPath=out_dir)
			try:
				attachment_df = pd.read_excel(out_dir + attachment.longFilename, sheet_name = "Loss History")
				if attachment_df["Entity"].empty:
					attachment_df["Entity"] = entity_sub
				if i == 0:
					df = attachment_df.copy()
					i = i + 1
				else:
					df = pd.concat([df, attachment_df])
			except:
				os.remove(out_dir + attachment.longFilename)
	return df

p = mp.Pool(4)
in_dir = "/data/lake/"
out_dir = "/data/lake/"
func = partial(extract_attachements, out_dir)
msg_files = [f for f in os.listdir(input_dir) if ".msg" in f]
mul_res = [p.apply_async(func, (f,)) for f in msg_files]
df = pd.concat([res.get() for res in mul_res])

# Get Crystel's NB Report to get the status


# Write to Optima SQL 
