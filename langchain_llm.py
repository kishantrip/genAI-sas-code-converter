from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage
from langchain.prompts.chat import ChatPromptTemplate
from code_migration import split_text
import os
from langchain.cache import SQLiteCache
from langchain.globals import set_llm_cache
set_llm_cache(SQLiteCache(database_path=".langchain.db"))


def prompt_generator(input_language_code, total_block, current_block,
                     input_language='sas', output_language='pyspark'):

    template = """Convert the {input_language} code to {output_language} code which is delimited with triple backticks.
    As the text is large the input will be provided in {total_block} parts. this is part{current_block}"""

    human_template = "```{input_language_code}```"

    chat_prompt = ChatPromptTemplate.from_messages([
        ("system", template),
        ("human", human_template),
    ])

    chat_prompt.format_messages(input_language=input_language, output_language=output_language,
                                total_block=total_block, current_block=current_block,
                                input_language_code=input_language_code)
    return chat_prompt

def openai_hit(text, input_language='sas', output_language='pyspark'):
    # llm = OpenAI()
    llm = ChatOpenAI(model="gpt-3.5-turbo")
    set_llm_cache(SQLiteCache(database_path=".langchain.db"))
    pysparkcode = []
    chunks = split_text(text)
    # t = prompt_generator(chunks)
    for i, item in enumerate(chunks):
        total_block = len(chunks)
        print(f'{i}/{total_block}')

        prompt = prompt_generator(item, total_block, i,
                                  input_language,
                                  output_language)
        chain = llm
        python_code = chain.invoke(input_language= item, total_block= total_block,
                                    current_block= i
                                    )
        pysparkcode.append(chain)
    converted_code = os.linesep.join([str(elem) for elem in pysparkcode])

if __name__ == "__main__":


