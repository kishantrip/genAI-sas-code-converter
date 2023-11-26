import streamlit as st
from PIL import Image
from code_migration import split_text, prompt_generator, get_completion
import openai
from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import time
import os
from io import StringIO
import re
from langchain.text_splitter import CharacterTextSplitter
from langchain.cache import InMemoryCache
from langchain_llm import chunk_generalize_checking_and_hitting
from langchain.globals import set_llm_cache

set_llm_cache(InMemoryCache())

im = Image.open('logo/IDFCFIRSTB.NS-6c6b4306.png')


def remove_comments(string):
    pattern = r"(\".*?(?<!\\)\"|\'.*?(?<!\\)\')|(/\*.*?\*/|//[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else:  # otherwise, we will return the 1st group
            return match.group(1)  # captured quoted-string

    return regex.sub(_replacer, string)


def click_button():
    st.session_state.clicked = True

def refresh_button():
    st.session_state.clicked = False
    # st.stop()


def smaller_chunk_size(item, pysparkcode, i):
    tmp_chunks = split_text(item, 1200)
    # t = prompt_generator(chunks)
    tmp = prompt_generator(tmp_chunks)
    for j, item1 in enumerate(tmp):
        print(f'{j + 1}sub/{len(tmp)}total')
        tmp_python_code = []
        with right:
            st.write(f'Chunk {i + 1} - {j + 1} processing began out of {len(tmp)} chunks')

        python_code = get_completion(item1, model="gpt-3.5-turbo")
        tmp_python_code.append(python_code)
        pysparkcode.extend(tmp_python_code)
    return pysparkcode


def txt_processing(upload_file):
    string_data = StringIO(upload_file.getvalue().decode("utf-8")).read()

    process_string = remove_comments(string_data)
    without_empty_lines = '\n'.join([
        line.strip() for line in process_string.splitlines()
        if line
    ])
    # with right:/
    # st.write(without_empty_lines)
    # string_data = re.sub(re.compile("/\*.*?\*/", re.DOTALL),
    #                      "", string_data)  # remove all occurrences streamed comments (/*COMMENT */) from string
    # return string_data
    return without_empty_lines


def code_migratrion_main(processed_code, max_chunk):
    template = """You are expert in converting sas code to pyspark code. All condition should be coded nothing to be 
    skipped Code is long so will be given in multiple parts which will be delimited with triple backticks. Create 
    spark session only in part1 of the code and skip for rest all parts."""

    human_template = "{code}"

    chat_prompt = ChatPromptTemplate.from_messages([
        ("system", template),
        ("human", human_template),
    ])

    model = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.0, request_timeout=240,
                       max_retries=0, cache=True)
    model2 = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.0, request_timeout=360,
                        max_retries=0, cache=True)

    chain = chat_prompt | model | StrOutputParser()

    regex_pat = re.compile(r'RUN;', flags=re.IGNORECASE)
    processed_code = re.sub(regex_pat, '**splitpoint**', processed_code)

    regex_pat = re.compile(r'QUIT;', flags=re.IGNORECASE)
    processed_code = re.sub(regex_pat, '**splitpoint**', processed_code)

    bb = CharacterTextSplitter(separator='**splitpoint**',
                               chunk_size=max_chunk,
                               chunk_overlap=0)

    code_chunk = bb.split_text(processed_code)

    code_chunk = [item.replace('**splitpoint**', 'run;') for item in code_chunk]

    prompts = [{"code": f'SAS Code Part{i + 1}: ```{item}```'} for i, item in enumerate(code_chunk)]

    open("converted_code.txt", "w+", encoding='utf-8')
    open("converted_code_sas.txt", "w+", encoding='utf-8')

    # print(len(prompts))
    range_loop = (len(prompts) / 2)
    if range_loop % 2 != 0:
        range_loop = int(range_loop) + 1
    else:
        range_loop = range_loop
    with right:
        st.write(f'Total Batch to process {range_loop}')

    t_0 = time.time()
    for i in range(range_loop):
        with right:
            st.write(f'Chunk {i+1} processing began out of {range_loop} chunk. {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}')
        try:
            print(i * 2, (i * 2) + 2)
            pyspark_code_tmp = chain.batch(prompts[i * 2: (i * 2) + 2])
            converted_code = os.linesep.join([str(elem) for elem in pyspark_code_tmp])
            with open("converted_code.txt", "a", encoding='utf-8') as f_py:
                f_py.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f_sas:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f_sas.write(converted_code_sas)

        except openai.APITimeoutError as e:
            # ('SAS code required!! Please upload a file.')
            st.error(e, icon="🚨")
            print(i * 2, (i * 2) + 2)
            chain2 = chat_prompt | model2 | StrOutputParser()
            pyspark_code_converted = chain2.batch(prompts[i * 2: (i * 2) + 2])
            converted_code = os.linesep.join([str(elem) for elem in pyspark_code_converted])
            with open("converted_code.txt", "a", encoding='utf-8') as f_py:
                f_py.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f_sas:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f_sas.write(converted_code_sas)

        except openai.BadRequestError as e:
            st.error(e, icon="🚨")
            print(i * 2, (i * 2) + 2)
            time.sleep(60)
            converted_code = chunk_generalize_checking_and_hitting(prompts[i * 2: (i * 2) + 2],
                                                                   max_chunk)
            with open("converted_code.txt", "a", encoding='utf-8') as f_py:
                f_py.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f_sas:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f_sas.write(converted_code_sas)
        except openai.error.RateLimitError:
            time.sleep(60)
            pyspark_code_tmp = chain.batch(prompts[i * 2: (i * 2) + 2])
            converted_code = os.linesep.join([str(elem) for elem in pyspark_code_tmp])
            with open("converted_code.txt", "a", encoding='utf-8') as f_py:
                f_py.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f_sas:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f_sas.write(converted_code_sas)

        except Exception as e:
            print(e)
            manual_conversion_text = '''\n\n\n########Manual conversion needed since context length is more than what 
            model allows###########\n {} \n##########manual block ends#############\n\n\n'''
            tmp = [manual_conversion_text.format(elem) for elem in prompts[i * 2: (i * 2) + 2]]
            converted_code = os.linesep.join(tmp)
            with open("converted_code.txt", "a", encoding='utf-8') as f_py:
                f_py.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f_sas:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f_sas.write(converted_code_sas)
        # Only needed if batch processing is done and RPM is exceeding
        with left:
            st.write(converted_code)
        # if i % 2 == 0:
        #     t_1 = time.time()
        #     if (t_1 - t_0) < 60:
        #         print(f"""put on sleep for matching RPM {(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))}""")
        #         time.sleep(60 - (t_1 - t_0))
        #         t_0 = t_1
    return converted_code


if __name__ == '__main__':
    st.set_page_config(layout="wide",
                       page_title="IDFC-GenAI-App",
                       initial_sidebar_state="expanded",
                       page_icon=im
                       )
    # st.image("logo/IDFC_First_Bank_logo.jpg", width=100)
    st.subheader('Demo SAS to Pyspark Code Converter')
    # st.subheader('Test')

    left, right = st.columns(2)
    st.markdown(
        """
        <style>
            div[data-testid="column"]:nth-of-type(1)
            {
                # border:1px solid red;
                border:1px;
            } 

            div[data-testid="column"]:nth-of-type(2)
            {
                # border:1px solid blue;
                text-align: center;
            } 
        </style>
        """, unsafe_allow_html=True
    )

    with left:
        uploaded_file = st.file_uploader("Choose a SAS Code Text File", ['txt', 'sas'])
        # submit_button = st.button(label='Upload file')
        # tt = StringIO(lines)

    if 'clicked' not in st.session_state:
        st.session_state.clicked = False

    with right:
        st.button('Convert', on_click=click_button)

    # with right:
    #     st.button('Refresh', on_click=refresh_button)

    if st.session_state.clicked:
        # The message and nested widget will remain on the page
        if uploaded_file is None:
            st.error('SAS code required!! Please upload a file.', icon="🚨")
        else:
            with right:
                st.write('Text File Processing Began!')
                with st.spinner('Wait for it...'):
                    lines = txt_processing(uploaded_file)
                    # with left:
                    # st.write(lines)
                    with open("processing_code.txt", "w", encoding='utf-8') as file:
                        file.write(lines)
                    st.write('Text Processing Completed')

                st.write('Conversion Began!')
                with st.spinner('Wait for it...'):
                    pyspark_code = code_migratrion_main(lines, 2500)
                    st.write('Conversion Done')
            # st.write(pyspark_code)
            with right:
                with open('converted_code.txt') as f:
                    # st.download_button('Download CSV', f)
                    st.download_button('Download Pyspark File',
                                       f,
                                       type="primary",
                                       file_name="pyspark.txt"
                                       )
        # else:
        #     st.error('SAS code required!! Please upload a file.', icon="🚨")

    st.session_state.clicked = False


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
