from langchain.llms import OpenAI
import os
import openai
from dotenv import load_dotenv
from langchain.document_loaders import TextLoader
import tiktoken
import re

# load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

def num_tokens_from_string(string: str, encoding_name: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens

def get_completion(prompt, model="gpt-3.5-turbo"):
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0,
    )
    return response.choices[0].message["content"]

def split_text(text):
    max_chunk_size = 2000
    chunks = []
    current_chunk = ""
    for sentence in text.split("run;"):
        if len(current_chunk) + len(sentence) < max_chunk_size:
            current_chunk += sentence + ""
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + "run;"
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

def prompt_generator(chunks):
    t = []
    for i in range(len(chunks)):
        #     if i==0:
        prompt = f"""
                Convert the sas code to pyspark which is delimited with triple backticks 
                As the text is large the input will be provided in {len(chunks)} parts. this is part{i + 1}

                Create a seprate function for every task 
                All the conditions must be coded do not skip any condition
                Dont use "# Add more conditions here..." code all the conditions

                {chunks[i]}
                """
        #     else:
        #         prompt= f'''`SAS Code`: chunk {i+1}
        #                 {chunks[i]}'''
        t.append(prompt)
    return t

if __name__ == "__main__":
    def code_migratrion_main(text):
        pysparkcode = []
        chunks = split_text(text)
        t = prompt_generator(chunks)
        for i, item in enumerate(t):
            print(f'{i}/{len(t)}')
            python_code = get_completion(item, model="gpt-3.5-turbo")
            pysparkcode.append(python_code)
        return t

    text = '''proc delete data=all;run; dm "clear log;clear list;"; options compress=yes symbolgen mlogic mprint stimer; libname ai "/SASVIYA_VSP/campmgt/MK_PL_Xsell/Jul23"; libname dm "/SASVIYA_VSP/campmgt/datamart"; %include '/sasdata/DB_PWD/DB_PWD.sas'; LIBNAME biumiror ODBC DSN="&BIUDMLI_PATH." USER="" PASSWORD="" SCHEMA=dbo; LIBNAME BIUBUDLI ODBC DSN="&BIUBUDLI_PATH." USER="&BIUBUDLI_USR." PASSWORD="" SCHEMA=dbo; LIBNAME dremio ODBC DSN="&dremio_path." USER="" PASSWORD="" schema='"'n; LIBNAME sfdc_DL ODBC DSN="&dremio_path." USER="" PASSWORD="" schema='""'n; libname datamart "/SASVIYA_VSP/campmgt/datamart"; libname biu1 "/BIUDATA_VA/campmgt/temp"; libname factmth "/BIUDATA_VA/campmgt/factmth"; libname dl_tbls "/biudata/Pradeep_kv"; data null; call symputx('REF_DT',put(intnx('MONTH',date(),-1,'E'),date9.)); call symputx('REF_DT1M',put(intnx('MONTH',date(),-2,'E'),date9.)); call symputx('REF_DT_L3M',put(intnx('MONTH',date(),-3,'E'),date9.)); call symputx('REF_DT_L6M',put(intnx('MONTH',date(),-6,'E'),date9.)); call symputx('REF_DT_L9M',put(intnx('MONTH',date(),-9,'B'),date9.)); call symputx('REF_DT_L12M',put(intnx('MONTH',date(),-12,'E'),date9.)); run; %put &REF_DT &REF_DT_L3M &REF_DT_L6M &REF_DT_L12M &REF_DT_L9M &REF_DT1M ; data fk; set dl_tbls.dimagreement; where datepart(disbursalDate) <= "&REF_DT."d and productsub = 'RC'; run; proc sql; create table fk1 as select a.,b.schemedesc from fk a left join dl_tbls.dimscheme b on a.schemeid = b.schemeid; quit; proc freq data=fk1;table schemedesc;run; data fk; set fk1; if schemeid in (61711) and compress(crn) ne ""; run; proc sql; create table ai.dimagr as select a. from dl_tbls.dimagreement a inner join fk b on a.crn = b.crn; quit; proc sql; create table dob1 as select crn,agreementid,customerid,disbursaldate from ai.dimagr where crn in(select crn from fk); run; proc sql; create table dob2 as select a.,b.dob from dob1 a left join dl_tbls.DIMCUSTOMER b on a.CUSTOMERID = b.CUSTOMERID; quit; data dob1; set dob2; if compress(crn) eq "" then delete; run; proc sort data=dob1;by crn descending DisbursalDate;run; proc sort data=dob1 nodupkey;by crn;run; proc sql; create table fk1 as select a.,b.dob as DOB2 from fk a left join dob1 b on compress(a.crn) = compress(b.crn); quit; proc sql; create table abc as select crn, max(dob2) as dob format date9., max(disbursalDate) as DISB_DT format date9., sum(DISBURSEDAMOUNT) as disb_amt from fk1 group by 1 ; quit; data ai.FK_adopter; set abc ; format tag1 $40.; if disb_amt > 100 then tag1 = "TRANSACTOR"; else tag1 = "ADOPTER"; where crn ne ''; run; data ai.fk_adopter_crn; set ai.FK_adopter; keep crn ; run; proc sort data=ai.FK_adopter nodupkey out=baseall2; by crn; run; proc sql; create table iq as select * from biu1.IQ_MST_current a where crn in (select input(crn, best12.) from ai.FK_adopter where crn ne ''); quit;'''

    tt = code_migratrion_main(text)