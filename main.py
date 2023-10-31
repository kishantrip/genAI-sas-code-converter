import pandas as pd
import streamlit as st
# import pandas as pd
import time
from PIL import Image
from io import StringIO
import re
import os


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
    st.write(without_empty_lines)
    return without_empty_lines
    # string_data = re.sub(re.compile("/\*.*?\*/", re.DOTALL),
    #                      "", string_data)  # remove all occurrences streamed comments (/*COMMENT */) from string
    # return string_data


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

            lines = txt_processing(uploaded_file)
            # with left:
            # st.write(lines)
            with open("processing_code.txt", "w", encoding='utf-8') as file:
                file.write(lines)

            # st.write(uploaded_file['name'])

            # st.slider('Select a value')
            with right:
                st.write('Text File Processing Began!')
                with st.spinner('Wait for it...'):
                    time.sleep(5)
                    st.write('Text Processing Completed')

                st.write('Conversion Began!')
                with st.spinner('Wait for it...'):
                    time.sleep(5)
                    st.write('Conversion Done')
            with right:
                with open('processing_code.txt') as f:
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
