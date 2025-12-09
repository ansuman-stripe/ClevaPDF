import os
from autohubble import hubble_query_to_df, PRESTO
from datetime import date
from pathlib import Path
import math
from pdfrw import PdfReader as PdfrwReader, PdfWriter as PdfrwWriter, PdfDict, PdfName, PdfObject, PdfArray
from pypdf import PdfReader as PypdfReader, PdfWriter as PypdfWriter
import pandas as pd
import csv

################################################################################################
# Pre Processing

# folder Configuration
script_directory = os.path.dirname(__file__)
template_pdf = os.path.join(script_directory, 'PerfectRecv_RetReqForm.pdf')
template_csv = os.path.join(script_directory, 'PerfectRecv_template.csv')

output_folder = os.path.join(script_directory, 'output_forms')
date_str = date.today().strftime('%Y_%m_%d')
form_folder = os.path.join(output_folder, f'{date_str}_ach_return')
Path(form_folder).mkdir(parents=True, exist_ok=True)
print(f"\nCreating forms in: {form_folder}")

# blockedfunding_list = input("Please enter the blockedfunding line items: ")
blockedfunding_list='blockedbtfunding_1SXGk6GkNNxmZIfz27oNJXDE, blockedbtfunding_1SXGkxGkNNxmZIfzzPVreaaC, blockedbtfunding_1SXGlnGkNNxmZIfzb3ctehPS, blockedbtfunding_1SXGjyGkNNxmZIfzHExrjQey, blockedbtfunding_1SXGjLGkNNxmZIfz7cwiD9Ao, blockedbtfunding_1SXybfGkNNxmZIfzWXkXD7qW, blockedbtfunding_1SXybnGkNNxmZIfz69uwVvyr, blockedbtfunding_1SXGlgGkNNxmZIfzFGXU68tm, blockedbtfunding_1SXq0lGkNNxmZIfzypItkiSF, blockedbtfunding_1SXdOWGkNNxmZIfz8QoIIIVX, blockedbtfunding_1SXdNsGkNNxmZIfzMvK9cuV7, blockedbtfunding_1SXGjmGkNNxmZIfzdmlgmiNT, blockedbtfunding_1SXGjqGkNNxmZIfzbuY2d7kt, blockedbtfunding_1SXdOeGkNNxmZIfzl4zDgvG7, blockedbtfunding_1SXGjHGkNNxmZIfzW0b17ZQH, blockedbtfunding_1SXGlrGkNNxmZIfz5NId6WbE, blockedbtfunding_1SXaJOGkNNxmZIfzx5Kstss2, blockedbtfunding_1SXGkEGkNNxmZIfzsC1xPlCg, blockedbtfunding_1SXGjuGkNNxmZIfzyNSC0bqj, blockedbtfunding_1SXGlQGkNNxmZIfzvNVGI2uZ, blockedbtfunding_1SXaJTGkNNxmZIfzyIpraWLi, blockedbtfunding_1SXGl5GkNNxmZIfzJFZEeqOP, blockedbtfunding_1SXTi7GkNNxmZIfzrVJtpLpe, blockedbtfunding_1SXGkAGkNNxmZIfzXDZCaEGi, blockedbtfunding_1SXGkMGkNNxmZIfz8r27NaXf, blockedbtfunding_1SXybjGkNNxmZIfzQX5HkDDW, blockedbtfunding_1SXdOSGkNNxmZIfzMhAStEXE, blockedbtfunding_1SXGlYGkNNxmZIfzXZ44oNVy, blockedbtfunding_1SXGl9GkNNxmZIfzXQ3FXvIq, blockedbtfunding_1SXycVGkNNxmZIfzwMqxiahD, blockedbtfunding_1SXGkpGkNNxmZIfzc21Z17Bw, blockedbtfunding_1SXGkIGkNNxmZIfzc4XIR5i4, blockedbtfunding_1SXwicGkNNxmZIfzXdJFO9km, blockedbtfunding_1SXGk2GkNNxmZIfzgeiWIZE2, blockedbtfunding_1SXGlcGkNNxmZIfznpMzcYKX, blockedbtfunding_1SXGjDGkNNxmZIfzgzts8kIr, blockedbtfunding_1SXGkUGkNNxmZIfzJX5Ce9vN, blockedbtfunding_1SXGjWGkNNxmZIfzOGyHFIA6, blockedbtfunding_1SXGl1GkNNxmZIfzk2Ek45Io, blockedbtfunding_1SXGklGkNNxmZIfznvY5iA4O, blockedbtfunding_1SXGjSGkNNxmZIfzn11zsMyl, blockedbtfunding_1SXdNHGkNNxmZIfzMaMhU6KG, blockedbtfunding_1SXGlvGkNNxmZIfzJLeXHa5o, blockedbtfunding_1SXGkPGkNNxmZIfzlaIZ90L8, blockedbtfunding_1SXGlUGkNNxmZIfzsx8KYTh5, blockedbtfunding_1SXGjaGkNNxmZIfz1PEabURk, blockedbtfunding_1SX7D3GkNNxmZIfzaPG7SCbz, blockedbtfunding_1SXdOOGkNNxmZIfzFQ6NRKPk, blockedbtfunding_1SXdNLGkNNxmZIfzEYoD61xA, blockedbtfunding_1SXq0fGkNNxmZIfzNOOs72mW, blockedbtfunding_1SXybcGkNNxmZIfzn2w6y4Ob, blockedbtfunding_1SXGjPGkNNxmZIfzzz1H9oVM, blockedbtfunding_1SXdNZGkNNxmZIfzUbu2Azxp, blockedbtfunding_1SX3KOGkNNxmZIfzLw92xFgs, blockedbtfunding_1SXGljGkNNxmZIfz4NXmtkBJ, blockedbtfunding_1SXGktGkNNxmZIfzvJcBlxFo, blockedbtfunding_1SXaJJGkNNxmZIfzukcQgBT6, blockedbtfunding_1SXaJEGkNNxmZIfzIxPTx8sR, blockedbtfunding_1SXyc2GkNNxmZIfzjasIrhXq, blockedbtfunding_1SXdNoGkNNxmZIfzJ5IirCPH'
formatted = ", ".join(f"'{item.strip()}'" for item in blockedfunding_list.split(','))
date_of_request = date.today().strftime('%m/%d/%Y')
requester = 'Ansuman Nayak' 
phone_number = '+91 7619344477'
requester_email = 'ansuman@stripe.com'
company_name = 'Stripe Inc'
group_number = '4063'

################################################################################################
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
)select blockedbtfunding, cash_obj, amount, wire_reference, vban, entry_date, 'Credit' as credit_or_debit, 'Checking' as checking_savings, 'R23--Credit Entry Refused by Receiver' as return_code from join_data
-- where resolution_status = 'pending'
'''

df_data = hubble_query_to_df(sql, PRESTO)

required_columns = ['return_code', 'entry_date', 'credit_or_debit', 'checking_savings', 'vban', 'amount', 'wire_reference']
entry_data = df_data[required_columns]

################################################################################################
# PDF Form Filling Functions

def prepare_pdf_data(row_data, index, date_of_request, requester, phone_number):
    
    # Format entry date from YYYY-MM-DD to MM/DD/YYYY
    entry_date = row_data['entry_date']
    if entry_date and '-' in str(entry_date):
        parts = str(entry_date).split('-')
        if len(parts) == 3:
            entry_date = f"{parts[1]}/{parts[2]}/{parts[0]}"
    
    # Prepare transaction data for this specific item
    data = {}
    item_prefix = f'item{index:02d}.'
    data[f'{item_prefix}code'] = 'R23'
    data[f'{item_prefix}effdate'] = entry_date
    data[f'{item_prefix}crdr'] = row_data['credit_or_debit']
    data[f'{item_prefix}chksav'] = row_data['checking_savings']
    data[f'{item_prefix}acct'] = str(row_data['vban'])
    data[f'{item_prefix}amt'] = f"{float(row_data['amount']):.2f}"
    data[f'{item_prefix}trace'] = str(row_data['wire_reference'])
    
    return data


def fill_pdf_form(template_path, output_path, form_data):
    """Fill PDF form using pdfrw library - keeps form editable"""
    try:
        pdf = PdfrwReader(template_path)
        
        print("\nFilling PDF form with data:")
        print(f"Total fields to fill: {len(form_data)}")
        
        # Get annotations from the page
        annotations = pdf.pages[0]['/Annots']
        filled_count = 0
        
        if annotations:
            for annotation in annotations:
                if annotation['/Subtype'] == '/Widget':
                    field_name = annotation['/T']
                    if field_name:
                        # Remove parentheses from field name
                        field_name_clean = field_name[1:-1] if field_name.startswith('(') else field_name
                        
                        if field_name_clean in form_data:
                            value = str(form_data[field_name_clean])
                            # Set the value
                            annotation.update(PdfDict(V=f'{value}'))
                            filled_count += 1
        
        print(f"Successfully filled {filled_count} fields")
        
        # Write the filled PDF
        writer = PdfrwWriter()
        writer.write(output_path, pdf)
        print(f"Successfully created PDF: {output_path}")
        return True
    except Exception as e:
        print(f"Error filling PDF: {e}")
        import traceback
        traceback.print_exc()
        return False

################################################################################################
# CSV Export Function

def export_to_csv(df, output_path):
    """Export remaining transactions to CSV file - simple format"""
    try:
        # Create a copy of the dataframe
        export_df = df.copy()
        
        # Format entry date from YYYY-MM-DD to MM/DD/YYYY
        export_df['entry_date'] = export_df['entry_date'].apply(
            lambda x: pd.to_datetime(x).strftime('%m/%d/%Y') if pd.notna(x) else ''
        )
        
        # Format amount with 2 decimal places
        export_df['amount'] = export_df['amount'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) else ''
        )
        
        # Rename columns to match expected format
        column_mapping = {
            'return_code': 'Return Code',
            'entry_date': 'Effective Date',
            'credit_or_debit': 'Credit/Debit',
            'checking_savings': 'Checking/Savings',
            'vban': 'Perfect Receivables Account Number',
            'amount': 'Dollar Amount',
            'wire_reference': 'Trace Number'
        }
        export_df = export_df.rename(columns=column_mapping)
        
        # Reorder columns
        column_order = [
            'Return Code',
            'Effective Date',
            'Credit/Debit',
            'Checking/Savings',
            'Perfect Receivables Account Number',
            'Dollar Amount',
            'Trace Number'
        ]
        export_df = export_df[column_order]
        
        # Export to CSV
        export_df.to_csv(output_path, index=False)
        
        print(f"Successfully created CSV file: {output_path}")
        return True
    except Exception as e:
        print(f"Error creating CSV file: {e}")
        import traceback
        traceback.print_exc()
        return False
    
################################################################################################
# Email Body Generation Function

def generate_email_body(entry_data, account_number='4941789844'):
    """Generate email body for Wells Fargo return request"""
    
    email_body = f"""Hi Bibiana,

We need to get the following funds returned to their originator, as they were credited to Stripe's account #{account_number}. Please see the transaction details below:

"""
    
    # Add each transaction
    for idx, row in entry_data.iterrows():
        # Format entry date from YYYY-MM-DD to YYYY-MM-DD (or custom format)
        entry_date = row['entry_date']
        if entry_date and '-' in str(entry_date):
            # Keep as YYYY-MM-DD or convert to MM/DD/YYYY
            parts = str(entry_date).split('-')
            if len(parts) == 3:
                entry_date = f"{parts[0]}-{parts[1]}-{parts[2]}"  # YYYY-MM-DD
                # Or use: entry_date = f"{parts[1]}/{parts[2]}/{parts[0]}"  # MM/DD/YYYY
        
        # Extract reason from return code
        return_code_full = row['return_code']
        if '--' in return_code_full:
            reason = return_code_full.split('--')[1]
        else:
            reason = return_code_full
        
        # Extract sequence number (last part of wire_reference)
        wire_reference = str(row['wire_reference'])
        sequence_number = wire_reference[-7:] if len(wire_reference) >= 7 else wire_reference
        
        transaction_num = idx + 1
        email_body += f"""Transaction {transaction_num}:
Amount: ${float(row['amount']):.2f}
Effective Date: {entry_date}
VBAN: {row['vban']}
Sequence number: {sequence_number}
Reason: {reason}

"""
    
    email_body += """Let me know if you have any questions!
Thank you"""
    
    return email_body


################################################################################################
# PDF Transaction Summary Function

def generate_pdf_transaction_summary(entry_data, pdf_count=10):
    """Generate a summary of transactions that went into the PDF"""
    
    actual_count = min(pdf_count, len(entry_data))
    pdf_transactions = entry_data.iloc[:actual_count]
    
    summary = f"""
{'='*80}
PDF TRANSACTION SUMMARY
{'='*80}
Total transactions in PDF: {actual_count}

"""
    
    for idx, row in pdf_transactions.iterrows():
        # Format entry date
        entry_date = row['entry_date']
        if entry_date and '-' in str(entry_date):
            parts = str(entry_date).split('-')
            if len(parts) == 3:
                entry_date = f"{parts[1]}/{parts[2]}/{parts[0]}"
        
        # Extract return code
        return_code_full = row['return_code']
        return_code = return_code_full.split('--')[0] if '--' in return_code_full else return_code_full
        
        transaction_num = idx + 1
        summary += f"""Transaction {transaction_num}:
  Return Code:    {return_code}
  Effective Date: {entry_date}
  Credit/Debit:   {row['credit_or_debit']}
  Account Type:   {row['checking_savings']}
  VBAN:           {row['vban']}
  Amount:         ${float(row['amount']):.2f}
  Trace Number:   {row['wire_reference']}
  Full Reason:    {return_code_full}

"""
    
    summary += f"{'='*80}\n"
    
    return summary

################################################################################################
# Main Processing

def process_transactions(entry_data, form_folder, date_of_request, requester, phone_number, 
                        company_name, group_number):
    """Process transactions: first 10 to PDF, rest to CSV"""
    
    total_transactions = len(entry_data)
    print(f"\nTotal transactions to process: {total_transactions}")
    
    if total_transactions == 0:
        print("No transactions to process!")
        return
    
    # Process first 10 transactions for PDF
    pdf_count = min(10, total_transactions)
    
    if pdf_count > 0:
        print(f"\nProcessing first {pdf_count} transactions for PDF form...")
        
        # Prepare all form data - start with header fields
        all_form_data = {
            'req.date': date_of_request,
            'req.requestor': requester,
            'req.phone': phone_number,
            'req.company': company_name,
            'req.group': group_number,
        }
        
        # Add each transaction
        for idx in range(pdf_count):
            row = entry_data.iloc[idx]
            transaction_data = prepare_pdf_data(row, idx + 1, date_of_request, requester, phone_number)
            all_form_data.update(transaction_data)
        
        # Fill and save PDF
        output_pdf = os.path.join(form_folder, f'ACH_Return_Request_{date_str}.pdf')
        fill_pdf_form(template_pdf, output_pdf, all_form_data)
        
        # Generate PDF transaction summary
        pdf_summary = generate_pdf_transaction_summary(entry_data, pdf_count)
        print(pdf_summary)
        
        # Save summary to file
        summary_file = os.path.join(form_folder, f'PDF_Transaction_Summary_{date_str}.txt')
        with open(summary_file, 'w') as f:
            f.write(pdf_summary)
        print(f"PDF transaction summary saved to: {summary_file}")
    
    # Process remaining transactions for CSV
    if total_transactions > 10:
        remaining_count = total_transactions - 10
        print(f"\nProcessing remaining {remaining_count} transactions for CSV...")
        
        remaining_data = entry_data.iloc[10:].reset_index(drop=True)
        output_csv = os.path.join(form_folder, f'ACH_Return_Request_Additional_{date_str}.csv')
        export_to_csv(remaining_data, output_csv)
    
    # Generate email body for ALL transactions
    print("\n" + "="*80)
    print("GENERATING EMAIL BODY")
    print("="*80)
    email_body = generate_email_body(entry_data)
    print(email_body)
    
    print(f"\n{'='*60}")
    print(f"Processing Complete!")
    print(f"{'='*60}")
    print(f"Total transactions processed: {total_transactions}")
    print(f"  - PDF form: {pdf_count} transactions")
    if total_transactions > 10:
        print(f"  - CSV file: {total_transactions - 10} transactions")
    print(f"  - PDF summary: {summary_file}")
    print(f"Output folder: {form_folder}")
    print(f"{'='*60}\n")

################################################################################################
# Execute the processing

if __name__ == "__main__":
    process_transactions(entry_data, form_folder, date_of_request, requester, phone_number,
                        company_name, group_number)