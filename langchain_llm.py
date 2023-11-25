from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
import time
import os
from io import StringIO
import re
from langchain.text_splitter import CharacterTextSplitter

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
    regex = re.compile(pattern, re.MULTILINE|re.DOTALL)

    def _replacer(match):
        # if the 2nd group (capturing comments) is not None,
        # it means we have captured a non-quoted (real) comment string.
        if match.group(2) is not None:
            return ""  # so we will return empty to remove the comment
        else: # otherwise, we will return the 1st group
            return match.group(1) # captured quoted-string
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

code = open("Mobikwik_Exe_Jul23.sas", "r").read()

processed_code = txt_processing(code)



# input_code_language = "sas"
# output_code_language = "pyspark"
# total_chunks = 2
# chunk_num = 1



# def prompt_generator(code_chunk, chunk_num, total_chunks, input_code_language, output_code_language):

template = f"""You are expert in converting sas code to pyspark code. All condition should be coded nothing to be skipped. 
Code is long so will be given in multiple parts which will be delimited with triple backticks.
Create spark session only in part1 of the code and skip for rest all parts.
"""

human_template = "{code}"


chat_prompt = ChatPromptTemplate.from_messages([
    ("system", template),
    ("human", human_template),
])

# chat_prompt.format_messages(input_code_language=input_code_language,
#                             output_code_language=output_code_language,
#                             total_chunks=total_chunks,
#                             chunk_num=chunk_num
#                             )

model = ChatOpenAI(model="gpt-3.5-turbo-16k", temperature=0)
chain = chat_prompt | model | StrOutputParser()
# chain = chat_prompt | model

# code_chunk = split_text(processed_code, max_chunk_size=2000)
regex_pat = re.compile(r'RUN;', flags=re.IGNORECASE)
processed_code = re.sub(regex_pat,'**splitpoint**', processed_code)

regex_pat = re.compile(r'QUIT;', flags=re.IGNORECASE)
processed_code = re.sub(regex_pat,'**splitpoint**', processed_code)

bb = CharacterTextSplitter(separator='**splitpoint**',
    chunk_size=2000,
    chunk_overlap=0)

code_chunk = bb.split_text(processed_code)

chunk_gt_2000 = [i for i, item in enumerate(code_chunk) if len(item)>2000]

code_chunk = [item.replace('**splitpoint**', 'run;') for item in code_chunk]

prompts = [{"code": f'SAS Code Part{i+1}: ```{item}```'} for i,item in enumerate(code_chunk)]


# pyspark_code = await chain.abatch(prompts[0:3], config={"max_concurrency": 3})
#
# converted_code = os.linesep.join([str(elem) for elem in pyspark_code])
# with open("converted_code.txt", "w", encoding='utf-8') as f:
#     f.write(converted_code)
file = open("converted_code.txt", "w+", encoding='utf-8')
file_sas = open("converted_code_sas.txt", "w+", encoding='utf-8')

print(len(prompts))
range_loop = (len(prompts)/2)
if range_loop%2 != 0:
    range_loop = int(range_loop) + 1
else:
    range_loop = range_loop

print(f'total loop {range_loop}')
t_0 = time.time()
for i in range(range_loop):
    print(f'loop number {i}')
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    try:
        print(i*2, (i*2)+2)
        pyspark_code = chain.batch(prompts[i*2: (i*2)+2])
        converted_code = os.linesep.join([str(elem) for elem in pyspark_code])
        with open("converted_code.txt", "a", encoding='utf-8') as f:
            f.write(converted_code)
        with open("converted_code_sas.txt", "a", encoding='utf-8') as f:
            converted_code_sas = os.linesep.join(
                [str(elem['code']) for elem in prompts[i*2: (i*2)+2]]
            )
            f.write(converted_code_sas)

        # for s in chain.stream(item):
        # #     print(s.content, end="", flush=True)

    except Exception as e:
        print(i*2, (i*2)+2)
        print(e)
        time.sleep(60)
        pyspark_code = chain.batch(prompts[i*2: (i*2)+2])
        converted_code = os.linesep.join([str(elem) for elem in pyspark_code])
        with open("converted_code.txt", "a", encoding='utf-8') as f:
            f.write(converted_code)
        with open("converted_code_sas.txt", "a", encoding='utf-8') as f:
            converted_code_sas = os.linesep.join(
                [str(elem['code']) for elem in prompts[i * 2: (i * 2) + 2]]
            )
            f.write(converted_code_sas)

    # if i%2==0:
    t_1 = time.time()
    if (t_1-t_0) < 60:
        print(f"put on sleep for matching RPM {print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))}")
        time.sleep(60-(t_1-t_0))
        t_0 = t_1


# pyspark_code = chain.invoke({"code": code})
