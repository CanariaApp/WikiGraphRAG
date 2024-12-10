## WikiGraph RAG
## Build Wikipedia Graph
### Get Wikidump
Use either the 2024_06_01 dump of English Wikipedia, or download the latest with
```bash
./src/data/raw_wikidump.sh 
```
### Get Embeddings
We will be using the BGE3 embeddings from https://huggingface.co/datasets/Upstash/wikipedia-2024-06-bge-m3
You can either stream them from Huggingface or download them in advance (faster):
```bash
git clone --no-checkout https://huggingface.co/datasets/Upstash/wikipedia-2024-06-bge-m3
cd .\wikipedia-2024-06-bge-m3\
git sparse-checkout init
git sparse-checkout set data/en
```
### Start Containers
Start the relevant containers from the project root directory with
```bash
docker-compose up -d
```
### Build DB
Start the relevant containers from the project root directory with
```bash
python .\src\parse\prepare_wiki_db.py <OPTIONS>"
```
If you have already downloaded the embeddings, add the location with the ```--embeddings_dir```
To create compressed csv files of nodes and references for future import into Neo4j, add ```--insert_nodes_csv --insert_edges_csv```

Note that this process can take a very long time. The script will go over the wikidump twice, first to collect redirects, and a second time to collect content (i.e., paragraphs) and match it with embeddings.

## Set up the LLM

### Using llama.cpp
1) Install Python bindings for llama.cpp to access API through Langchain
```bash
pip install llama-cpp-python
```
2) Download an LLM model from https://huggingface.co/models. It should be in the .gguf format otherwise you will need to convert it (see https://github.com/ggerganov/llama.cpp for converters)


### Using vLLM
Run a vLLM server in the background (requires Linux), below is an example
```
python -m vllm.entrypoints.openai.api_server \
	--model microsoft/Phi-3-mini-128k-instruct \
	--max-model-len 4096 \
	--dtype bfloat16 \
	--gpu-memory-utilization 0.90 \
	--port=8002 \
	--trust-remote-code \
	--disable-log-stats
```
Depending on your system you might want to change the parameters.


## Usage
If you are using llama.cpp run:
```bash
python /src/app.py llamacpp <path_to_your_model_file>
```

If you are using vLLM run:
```bash
python /src/app.py vllm <name_of_the_model>
``` 

Once the graph RAG model has started you can use a browser to access the gradio interface it generates at http://localhost:7860 .

Once a question is submitted the script will do a vector search through the nodes (i.e., wiki paragraphs) in the database. These will be the starting points for the random walk algorithm that follows links within the paragraphs to find more relevant context.

Finally, the LLM is given two prompts: one without context and one with the context provided by the posts returned by the search.