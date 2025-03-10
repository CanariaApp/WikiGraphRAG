import sys
from dotenv import load_dotenv
import time
import pandas as pd
from src.infra.connections_mongodb import MongoDBJobDB
import numpy as np
from sentence_transformers import SentenceTransformer
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
import gradio as gr
from rag_base import retriever

# Load environment variables
load_dotenv()

LLM_LIBRARY = sys.argv[1]
LLM_MODEL = sys.argv[2]
INDEX_PATH = sys.argv[3] #"src/data/admin/bge3_index.pkl"

#Connect to DB
mongodb_client = MongoDBJobDB(
    "mongodb://localhost:27018/",
#"mongodb://76.24.32.213:27018/",
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
    start_time = time.time()
    try:
        if documents is None:
            context = ""
            template = """Please provide an answer to the question. Keep the answer as short as possible. Respond "Unsure" if not sure about the answer. 
            {context}

            Question: {question}

            Answer:"""
        else:
            context = " \n ".join([doc["content"][0][:max_context_length] for doc in documents])
            template = """Given the following detailed context from Wikipedia pages, 
            please provide an answer to the question. Keep the answer as short as possible. Respond "Unsure" if not sure about the answer. 
    
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
        print(f"LLM Response: {response[:max_answer_length].split("\n")[0]}")
        #Return only the first line of the answer
        return response[:max_answer_length].split("\n")[0], (time.time() - start_time)

    except Exception as e:
        print(f"Error generating answer: {e}")
        return f"Sorry, I couldn't generate an answer. Error: {str(e)}", (time.time() - start_time)



def wiki_qa(question, top_k=3, max_answer_length=100, n_walks=3, max_steps=3, max_context_doc_size=500):
        #try:
        # Perform hybrid search
        basic_docs, all_docs, vector_time, walk_time = retriever(question, embeddingfunc,embeddingfunc_direct_search,
                                                                 mongodb_client.query_df, INDEX_PATH, n_walks=n_walks,
                                                                 max_steps=max_steps, k_best=top_k)

        # Generate answer using LLM without context
        answer_no_context, answer_no_context_time = generate_answer(question, None,max_answer_length=max_answer_length)

        # Generate answer using LLM with hybrid results
        answer_basic_context, answer_time_basic = generate_answer(question, basic_docs,max_answer_length=max_answer_length)

        # Generate answer using LLM with hybrid results
        answer_full_context, answer_time_full = generate_answer(question, all_docs,max_answer_length=max_answer_length)



        # Prepare document display for results
        basic_context = "\n\n".join(
            [
                f"Document {i + 1}:\n"+
                f"Content: {doc['content'][0][:max_context_doc_size]}...\n"
                for i, doc in enumerate(basic_docs)
            ]
        )
        # Prepare document display for results
        full_context = "\n\n".join(
            [
                f"Document {i + 1}:\n"+
                f"Content: {doc['content'][0][:max_context_doc_size]}...\n"
                for i, doc in enumerate(all_docs)
            ]
        )

        # Prepare timing information
        timing_info = (f"Vector Search Time: {vector_time:.4f} seconds \n\n" +
                       f"Random Walk Time: {walk_time:.4f} seconds \n\n" +
                       f"LLM Time (no context): {answer_no_context_time:.4f} seconds \n\n" +
                       f"LLM Time (basic context): {answer_time_basic:.4f} seconds \n\n" +
                       f"LLM Time (full context): {answer_time_full:.4f} seconds \n\n")


        return answer_no_context, answer_basic_context, answer_full_context, basic_context,full_context, timing_info

        # except Exception as e:
        #     error_message = f"An error occurred: {type(e).__name__}, {str(e)}"
        #     print(error_message)
        #     return error_message, "", ""


# Define the Gradio interface
gr_interface = gr.Interface(
    fn=wiki_qa,
    inputs=[gr.Textbox(lines=2, placeholder="Ask a question...")],
    outputs=[
        gr.Textbox(label="Generated Answer without context"),
        gr.Textbox(label="Generated Answer with basic context"),
        gr.Textbox(label="Generated Answer with full context"),
        gr.Markdown(label="Context from Vector Search"),
        gr.Markdown(label="Context from Vector Search and Random Walk"),
        gr.Markdown(label="Search Timing Information"),
    ],
    title="Wiki GraphRAG Application with Random Walk",
    description="Ask questions and get answers using a Retrieval-Augmented Generation model with a graph random walk on Wikipedia pages.",
)

if __name__ == "__main__":

    #Define embedding function to use
    sentence_transformer_model = SentenceTransformer(
        "BAAI/bge-m3",
        revision="babcf60cae0a1f438d7ade582983d4ba462303c2",
        device="cpu", #cuda
    )
    def embeddingfunc_direct_search(text):
        return sentence_transformer_model.encode(sentences=text,show_progress_bar=False,normalize_embeddings=True)

    # # Distill a Model2Vec model from a Sentence Transformer model
    # from model2vec.distill import distill
    # from model2vec import StaticModel
    #
    # # Load the model
    # model_name = "bge-m3_m2v_model"
    # try:
    #     m2v_model = StaticModel.from_pretrained(model_name)
    # except:
    #     print(f"Model2Vec model {model_name} not found, trying to distill it from Sentence Transformers...")
    #     start_time = time.time()
    #     m2v_model = distill(model_name="BAAI/bge-m3",pca_dims=1024)
    #     # Save the model
    #     m2v_model.save_pretrained(model_name)
    #     print(f"Distilling transformer model to Model2Vec took {(time.time() - start_time)/60} minutes")
    #
    # def embeddingfunc(text):
    #     return m2v_model.encode(sentences=text,show_progress_bar=False,normalize_embeddings=True)

    from model2vec import StaticModel
    # Load a pretrained Model2Vec model
    model2vec_model = StaticModel.from_pretrained("minishlab/potion-base-8M")
    # Compute text embeddings

    def embeddingfunc(text):
        return model2vec_model.encode(text)


    #question = "How many people live in New York"
    #start_ids, dt = vector_similarity_search(question, top_k=3)
    #res = [mongodb_client.query_df('pages', {"id": par_id}, {}, 1) for par_id in start_ids]

    # print(wiki_qa("How many people live in New York City?", top_k=3, max_answer_length=100,n_walks=1, max_steps=3))
    #
    # print(wiki_qa("If my future wife has the same first name as the 15th first lady of the United States'"
    #               " mother and her surname is the same as the second assassinated president's mother's maiden name,"
    #               " what is my future wife's name?", top_k=3, max_answer_length=100, n_walks=1, max_steps=3)) #Jane Ballou

    # print(wiki_qa("As of 2010, if you added the number of times Brazil had won the World Cup to the amount of times the Chicago Bulls "
    #               "had won the NBA Championship and multiplied this number by the amount of times the Dallas Cowboys had won the Super Bowl,"
    #               " what number are you left with?", top_k=3, max_answer_length=100, n_walks=1, max_steps=3)) #55

    # print(wiki_qa("I am thinking of a Ancient Roman City. The city was destroyed by volcanic eruption."
    #               " The eruption occurred in the year 79 AD. The volcano was a stratovolcano. "
    #               "Where was the session held where it was decided that the city would be named a UNESCO world heritage site?",
    #               top_k=3, max_answer_length=100, n_walks=1, max_steps=3)) #Naples




    #Start gradio interface
    gr_interface.launch()




    # #res = mongodb_client.query_df('pages', {"id" : 'anarchism_0'}, {}, 1)
    # print(embeddingfunc("test"))
    #
    # from time import sleep
    # sleep(10)
    #
    # print(embeddingfunc("nnnnn"))
    # a=2
    # b=3
    # sleep(10)