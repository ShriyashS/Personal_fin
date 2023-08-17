import datetime as dt
import numpy as np
import pandas as pd
from pathlib import Path
import tabula
from typing import List

class bank_dataframe_extraction():
    def get_file_path(self) -> str:
        """This function use to get the file"""
        curr_wd = Path(Path.cwd())
        for file in curr_wd.glob('*.pdf'):
            return str(file)
    
    def extract_info(self) -> List:
        """This function is used to pdf file"""
        table= tabula.read_pdf(self.get_file_path(), pages='all')
        return table
    
    def data_processing(self, list_data_dfs : List) -> pd.DataFrame:
        """Data cleaning"""
        data_df = pd.concat(list_data_dfs)
        #Renaming columns
        data_df.rename(columns = {'Txn Date':'TRANSACTION_DATE',
                                  'Value\rDate':'REF_DATE',
                                  'Description':'TRANSACTION_DESC',
                                  'Ref No./Cheque\rNo.':'REF_NO',
                                  'Debit':'DEBIT',
                                  'Credit':'CREDIT',
                                  'Balance':'BALANCE'}, inplace = True)
        #TYPE CASTING
        return  (
            (data_df
             .assign( TRANSACTION_DATE = lambda x : pd.to_datetime(x.TRANSACTION_DATE, format='%d %b %Y') )
             .assign( REF_DATE = lambda x : pd.to_datetime(x.REF_DATE, format='%d %b %Y') )
             .assign( DEBIT = lambda x : pd.to_numeric(x.DEBIT.str.replace(',', ''), errors='coerce') )
             .assign( CREDIT = lambda x : pd.to_numeric(x.CREDIT.str.replace(',', ''), errors='coerce') )
             .assign( BALANCE = lambda x : pd.to_numeric(x.BALANCE.str.replace(',', ''), errors='coerce') )
            )
        )
    

if __name__ == '__main__':
    bank_obj = bank_dataframe_extraction()
    input_list_dfs = bank_obj.extract_info()
    output_df = bank_obj.data_processing(input_list_dfs)