import openai
from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import time
import os
# from io import StringIO
import re
from langchain.text_splitter import CharacterTextSplitter
from langchain.cache import InMemoryCache
from langchain.globals import set_llm_cache

set_llm_cache(InMemoryCache())


# chat_model = ChatOpenAI()

def split_text(text, max_chunk_size=2000):
    # max_chunk_size = 2000
    chunks = []
    current_chunk = ""
    for sentence in (text.lower().split("run;")):
        if len(current_chunk) + len(sentence) < max_chunk_size:
            current_chunk += sentence + ""
        else:
            chunks.append(current_chunk.strip())
            current_chunk = sentence + "run;"
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks


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


def txt_processing(upload_file):
    # string_data = StringIO(upload_file.getvalue().decode("utf-8")).read()
    string_data = upload_file
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


def _context_length_check(chunks: list, max_length: int):
    return [i for i, item in enumerate(chunks) if len(item) > max_length]


def chunk_generalize_checking_and_hitting(prompt, max_chunk):
    manual_conversion_text = '''\n\n\n########Manual conversion needed since context length is more than what model 
    allows###########\n {}
    ##########manual block ends#############\n\n\n'''
    breach_index_ = _context_length_check(prompt, max_chunk)
    if len(breach_index) == 0:
        pyspark_code_llm = chain.batch(prompt)
        converted_code_text = os.linesep.join([str(elem) for elem in pyspark_code_llm])
    else:
        pyspark_code_llm = chain.batch([item for i, item in enumerate(prompt) if i not in breach_index_])
        pyspark_code_llm_tmp = pyspark_code_llm.copy()
        tmp_converted_code = [pyspark_code_llm_tmp.pop(0) if i not in breach_index_ else item
                              for i, item in enumerate(prompt)]
        converted_code_text = os.linesep.join(tmp_converted_code)
    return converted_code_text


if __name__ == "__main__":
    code = open("Mobikwik_Exe_Jul23.sas", "r").read()
    max_chunk = 2500

    processed_code = txt_processing(code)  # for removing comments

    template = """You are expert in converting sas code to pyspark code. All condition should be coded nothing to be skipped
    Code is long so will be given in multiple parts which will be delimited with triple backticks.
    Create spark session only in part1 of the code and skip for rest all parts.
    """

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

    breach_index = _context_length_check(code_chunk, max_chunk)

    code_chunk = [item.replace('**splitpoint**', 'run;') for item in code_chunk]

    prompts = [{"code": f'SAS Code Part{i + 1}: ```{item}```'} for i, item in enumerate(code_chunk)]

    file = open("converted_code.txt", "w+", encoding='utf-8')
    file_sas = open("converted_code_sas.txt", "w+", encoding='utf-8')

    print(len(prompts))
    range_loop = (len(prompts) / 2)
    if range_loop % 2 != 0:
        range_loop = int(range_loop) + 1
    else:
        range_loop = range_loop

    print(f'total loop {range_loop}')

    t_0 = time.time()
    for i in range(range_loop):
        print(f'loop number {i}')
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        try:
            print(i * 2, (i * 2) + 2)
            pyspark_code = chain.batch(prompts[i * 2: (i * 2) + 2])
            converted_code = os.linesep.join([str(elem) for elem in pyspark_code])
            with open("converted_code.txt", "a", encoding='utf-8') as f:
                f.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f.write(converted_code_sas)

        except openai.APITimeoutError as e:
            print(e)
            print(i * 2, (i * 2) + 2)
            chain2 = chat_prompt | model2 | StrOutputParser()
            pyspark_code = chain2.batch(prompts[i * 2: (i * 2) + 2])
            converted_code = os.linesep.join([str(elem) for elem in pyspark_code])
            with open("converted_code.txt", "a", encoding='utf-8') as f:
                f.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f.write(converted_code_sas)

        except openai.BadRequestError as e:
            print(e)
            print(i * 2, (i * 2) + 2)
            time.sleep(60)
            converted_code = chunk_generalize_checking_and_hitting(prompts[i * 2, (i * 2) + 2],
                                                                   max_chunk)
            with open("converted_code.txt", "a", encoding='utf-8') as f:
                f.write(converted_code)
            with open("converted_code_sas.txt", "a", encoding='utf-8') as f:
                converted_code_sas = os.linesep.join(
                    [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
                )
                f.write(converted_code_sas)
        # Only needed if batch processing is done and RPM is exceeding
        if i % 2 == 0:
            t_1 = time.time()
            if (t_1 - t_0) < 60:
                print(f"""put on sleep for matching RPM {(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))}""")
                time.sleep(60 - (t_1 - t_0))
                t_0 = t_1

    # pyspark_code = chain.invoke({"code": code})
