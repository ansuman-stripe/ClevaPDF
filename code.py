import os
from autohubble import hubble_query_to_df, PRESTO
from datetime import date
from pypdf import PdfReader, PdfWriter
from pathlib import Path
import math

script_directory = os.path.dirname(__file__)

blockedfunding_list = input("Please enter the blockedfunding line items: ")
formatted = ", ".join(f"'{item.strip()}'" for item in blockedfunding_list.split(','))

date_of_request = date.today().strftime('%m/%d/%Y')
requester = 'Ansuman Nayak' 
phone_number = '111111'

################################################################################################ Creating master data table for processing
# Creating master data table for processing

sql = f'''
with blocked_bt as (
  select from_unixtime(created) as created, _id as blockedbtfunding, external_id, blocked_reason, resolution_status from payserver_snapshot.blocked_bank_transfer_fundings
  where _id in ({formatted})
), funding_cte as (
  select id as fnm ,incoming_transaction_id, transaction_event_id, funds_activity_event_data.amount.amount as funded_amount, transaction_guaranteed_transaction_id as pytx, funds_activity_event_data.external_id as fpibtvnm from iceberg.h_merchant_banktransfersfpi.sharded_funds_network_model_records
  where 1=1
  	and transaction_guaranteed_transaction_id in (select external_id from blocked_bt)
), intxn_cte as (
  select id as intxn, source_id from iceberg.incomingtxndb.incoming_transaction_records
  where 1=1
  	and id in (select incoming_transaction_id from funding_cte)
),cashwfibai_cte as ( 
  select id as cash_obj, 
    regexp_extract(file_id,'of_.*') as ops_file,
    line_number, 'WIRE' as type, data,
    cast(data.transaction.amount as bigint)/100.00 as amount,
    regexp_extract(regexp_replace(data.transaction.text, ' ', ''), 'WIREREFERENCE:([0-9]{12})', 1) AS wire_reference,
    regexp_extract(data.transaction.text, 'BNF=(\d+)', 1) as vban,
    regexp_extract(data.transaction.text, 'OGB=([^\n]+)ORG=', 1) AS originating_bank,
    regexp_extract(data.transaction.text, 'ORG=(.*?)(?=RFB=|OBI=|OPI=)', 1) AS originator,
    trim(regexp_extract(data.transaction.text, 'BNF=[0-9]+ ([A-Za-z]+(?:\s+[A-Za-z]+){0,2})',1)) AS receiver,
    date_format(from_unixtime(data.group_header.as_of_date.millis/1000),'%Y-%m-%d') AS entry_date,
  	data.transaction.type_code,
    data.transaction.text AS full_description
  from iceberg.cashreportingdb.wells_fargo_intraday_bai_records
  where 1=1
  	and id in (select source_id from intxn_cte)
), cashwfpr_cte as (
  select id as cash_obj, 
	regexp_extract(file_id,'of_.*') as ops_file,
	line_number, concat('ACH ',type) as type, 
  cast(coalesce(domestic.entry.amount,international.addendum710.destination_amount )as bigint)/100.00 as amount,
  coalesce(concat(domestic.entry.originating_dfi_id,domestic.entry.sequence_number),concat(international.entry.originating_dfi_id,international.entry.sequence_number)) as wire_reference,
  coalesce(domestic.entry.account_number,international.entry.account_number) as vban,
  coalesce(domestic.batch_header.company_name, international.batch_header.company_name) as company_name,
  coalesce(domestic.batch_header.company_id, international.batch_header.company_id) as company_id,
  coalesce(domestic.entry.individual_name, international.addendum710.receiver_name) as individual_name,
  domestic.entry.individual_id as individual_id,
  date_format(from_unixtime(coalesce(domestic.batch_header.effective_entry_date.millis,international.batch_header.effective_entry_date.millis)/1000),'%Y-%m-%d') as entry_date,
  if(coalesce(domestic.entry.transaction_code, international.entry.transaction_code) in (
      'SAVINGS_PRENOTE_CREDIT',
      'SAVINGS_DEBIT_RETURN',
      'SAVINGS_CREDIT',
      'PRENOTE_DEBIT',
      'PRENOTE_CREDIT',
      'DEBIT_RETURN',
      'CREDIT'),'credit','debit') as type_code
	from iceberg.cashreportingdb.wells_fargo_perfect_receivable_records
  where 1=1
  	and id in (select source_id from intxn_cte)
), wire_ach_join as (
  select cash_obj, ops_file, line_number, type_code, type, amount, wire_reference, vban, company_name as originator, individual_name as receiver, entry_date,
  json_object('company_id' value company_id, 'individual_id' value individual_id) as additional_transaction_info
  from cashwfpr_cte 
  union 
  select cash_obj, ops_file, line_number, type_code, type, amount, wire_reference, vban, originator, receiver, entry_date,
  json_object('originating_bank' value originating_bank) as additional_transaction_info
  from cashwfibai_cte 
), join_data as (
  select * from blocked_bt bb 
  left join funding_cte fc on fc.pytx = bb.external_id
  left join intxn_cte ic on fc.incoming_transaction_id = ic.intxn
  left join wire_ach_join waj on ic.source_id = waj.cash_obj
)select blockedbtfunding, cash_obj, amount, wire_reference, vban, entry_date from join_data
where resolution_status = 'pending'
'''
df_data = hubble_query_to_df(sql, PRESTO)

required_columns = ['entry_date', 'vban', 'amount', 'wire_reference']
entry_data = df_data[required_columns]
print(entry_data)

################################################################################################
# Fill PDF Forms

# folder Configuration
output_folder = os.path.join(script_directory, 'output_forms')
date_str = date.today().strftime('%Y_%m_%d')
form_folder = os.path.join(output_folder, f'{date_str}_ach_return')
Path(form_folder).mkdir(parents=True, exist_ok=True)
print(f"\nCreating forms in: {form_folder}")

# PDF form processing 
template_pdf = os.path.join(script_directory, 'PerfectRecv_RetReqForm.pdf') 
rows_per_form = 10
total_transactions = len(entry_data)
num_forms = math.ceil(total_transactions / rows_per_form)

print(f"Total transactions: {total_transactions}")
print(f"Forms to create: {num_forms}")
print()

for form_num in range(num_forms):
    start_idx = form_num * rows_per_form
    end_idx = min(start_idx + rows_per_form, total_transactions)

    print(f"Creating form {form_num + 1}/{num_forms} (transactions {start_idx + 1} to {end_idx})...")

    # initializing PDF
    reader = PdfReader(template_pdf)
    writer = PdfWriter()
    writer.append(reader)

    # Prepare form data
    form_data = {}
    form_data['req.date'] = date_of_request
    form_data['req.requestor'] = requester
    # form_data['req.phone'] = phone_number

    # Fill transaction rows
    for i, row_idx in enumerate(range(start_idx, end_idx), start=1):
        row = entry_data.iloc[row_idx]
        
        # Data formating
        entry_date = row['entry_date']
        if isinstance(entry_date, str) and '-' in entry_date:
            parts = entry_date.split('-')
            entry_date = f"{parts[1]}/{parts[2]}/{parts[0]}"
        amount = f"{float(row['amount']):.2f}" 

        item = f'item{i:02d}' # Item prefix (item01, item02, etc.)

        form_data[f'{item}.crdr'] = 'Credit'
        form_data[f'{item}.chksav'] = 'Checking' 
        form_data[f'{item}.effdate'] = entry_date
        form_data[f'{item}.code'] = 'R23'
        form_data[f'{item}.acct'] = row['vban']
        form_data[f'{item}.amt'] = amount
        form_data[f'{item}.trace'] = row['wire_reference']
    
    # Update the form fields
    writer.update_page_form_field_values(
        writer.pages[0], 
        form_data
    )
    # Save the filled form
    output_filename = f'PerfectRecv_RetReqForm_{date_str}_{form_num + 1}.pdf'
    output_path = os.path.join(form_folder, output_filename)
    
    with open(output_path, 'wb') as output_file:
        writer.write(output_file)
    
    print(f"Created: {output_filename}")

print()
print(f"Successfully created {num_forms} form(s)")
