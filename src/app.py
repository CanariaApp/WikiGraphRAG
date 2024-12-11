import sys
from dotenv import load_dotenv
import time
import pandas as pd
from src.infra.connections_mongodb import MongoDBJobDB
from sentence_transformers import SentenceTransformer
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
import gradio as gr

# Load environment variables
load_dotenv()

LLM_LIBRARY = sys.argv[1]
LLM_MODEL = sys.argv[2]

#Connect to DB
mongodb_client = MongoDBJobDB(
    "mongodb://localhost:27018/",
    "wikidump5x",
)

# Set up the language model
if (LLM_LIBRARY == 'llamacpp'):
    from langchain_community.llms.llamacpp import LlamaCpp

    model_path = LLM_MODEL  # example "openhermes-2.5-mistral-7b.Q4_K_M.gguf"
    llm = LlamaCpp(
        model_path=model_path,
        temperature=0.5,
        max_tokens=1000,
        n_ctx=1024,
        n_gpu_layers=512,
    )
elif (LLM_LIBRARY == 'vllm'):

    from langchain_openai import OpenAI

    API_KEY = "EMPTY"
    BASE_URL = "http://localhost:8002/v1"
    model_name = LLM_MODEL  # example "microsoft/Phi-3-mini-128k-instruct"

    llm = OpenAI(
        api_key="EMPTY",
        base_url="http://localhost:8002/v1",
        model_name=model_name,
    )
    # from langchain_community.llms import VLLM
    # llm = VLLM(
    #     model=model_name,
    #     trust_remote_code=True,
    #     max_model_len=512,
    #     top_k=10,
    #     top_p=0.95,
    #     temperature=0.1,
    #     gpu_memory_utilization=0.95,
    #     dtype="bfloat16",
    # )
else:
    print(" Unknown parameter for LLM_LIBRARY: %s\n Please use either llamacpp or vllm" % (LLM_LIBRARY))
    exit()


#Function to ask the LLM with context
def generate_answer(
        question, documents, max_context_length=500, max_answer_length=1000
):
    try:
        if documents is None:
            context = ""
        else:
            context = " ".join([doc["post"][:max_context_length] for doc in documents])

        template = """Given the following detailed context from Stack Overflow posts, 
        please provide a comprehensive and well-explained answer to the question. 
        Make sure to cover different aspects and provide examples if possible

        Context:
        {context}

        Question: {question}

        Answer:"""

        prompt = PromptTemplate(
            template=template,
            input_variables=["context", "question"],
        )

        chain = (
                {
                    "context": RunnablePassthrough(),
                    "question": RunnablePassthrough(),
                }
                | prompt
                | llm
        )

        response = chain.invoke({"context": context, "question": question})
        print(f"LLM Response: {response[:max_answer_length]}")
        return response

    except Exception as e:
        print(f"Error generating answer: {e}")
        return f"Sorry, I couldn't generate an answer. Error: {str(e)}"

def vector_similarity_search(question, top_k=3):
    start_time = time.time()
    results = similarity_search_with_score(question, k=top_k)
    search_time = time.time() - start_time

    return results, search_time

def stackoverflow_qa(question,top_k=3):
    try:
        # Perform hybrid search
        results, vector_time = vector_similarity_search(question,top_k=top_k)

        # Generate answer using LLM with hybrid results
        answer = generate_answer(question, results)

        # Generate answer using LLM without context
        answer_no_context = generate_answer(question, None)

        # Prepare document display for results
        doc_display = "\n\n".join(
            [
                f"Document {i + 1}:\n"
                f"Content: {doc['post'][:500]}...\n"
                f"Similarity Score: {doc['score']:.4f}"
                for i, doc in enumerate(results)
            ]
        )

        # Prepare timing information
        timing_info = f"Vector Search Time: {vector_time:.4f} seconds"

        return answer_no_context, answer, doc_display, timing_info

    except Exception as e:
        error_message = f"An error occurred: {type(e).__name__}, {str(e)}"
        return error_message, "", ""


# Define the Gradio interface
gr_interface = gr.Interface(
    fn=stackoverflow_qa,
    inputs=[gr.Textbox(lines=2, placeholder="Ask a question...")],
    outputs=[
        gr.Textbox(label="Generated Answer without context"),
        gr.Textbox(label="Generated Answer with context"),
        gr.Markdown(label="Hybrid Search Results"),
        gr.Markdown(label="Search Timing Information"),
    ],
    title="Wiki GraphRAG Application with Random Walk",
    description="Ask questions and get answers using a Retrieval-Augmented Generation model with a graph random walk on Wikipedia pages.",
)

if __name__ == "__main__":

    transformer = SentenceTransformer(
        "BAAI/bge-m3",
        revision="babcf60cae0a1f438d7ade582983d4ba462303c2",
        device="cpu", #cuda
    )
    def embeddingfunc(text):
        return transformer.encode(sentences=text,show_progress_bar=False,normalize_embeddings=True)

    #Start gradio interface
    gr_interface.launch()




    #res = mongodb_client.query_df('pages', {"id" : 'anarchism_0'}, {}, 1)
    print(embeddingfunc("test"))

    from time import sleep
    sleep(10)

    print(embeddingfunc("nnnnn"))
    a=2
    b=3
    sleep(10)