import pandas as pd
import streamlit as st
# import pandas as pd
import time
from PIL import Image
from io import StringIO
import re
import os
from code_migration import split_text, prompt_generator, get_completion


im = Image.open('logo/IDFCFIRSTB.NS-6c6b4306.png')

def remove_comments(string):
    pattern = r"(\".*?(?<!\\)\"|\'.*?(?<!\\)\')|(/\*.*?\*/|//[^\r\n]*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE|re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else: # otherwise, we will return the 1st group
            return match.group(1) # captured quoted-string
    return regex.sub(_replacer, string)


def click_button():
    st.session_state.clicked = True


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



def code_migratrion_main(text):
    pysparkcode = []
    chunks = split_text(text)
    t = prompt_generator(chunks)
    for i, item in enumerate(t):
        print(f'{i+1}/{len(t)}')
        with right:
            st.write(f'Chunk {i+1} processing began out of {len(t)} chunks')
        python_code = get_completion(item, model="gpt-3.5-turbo")
        pysparkcode.append(python_code)
    converted_code = os.linesep.join([str(elem) for elem in pysparkcode])
    with open("converted_code.txt", "w", encoding='utf-8') as f:
        f.write(converted_code)
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
                    pyspark_code = code_migratrion_main(lines)
                    st.write('Conversion Done')
            st.write(pyspark_code)
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
