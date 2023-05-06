#------------Imports---------------
import os
import openai
import pandas as pd
import hashlib
import numpy as np
import logging

from langchain.document_loaders import PyPDFLoader
from langchain.document_loaders import UnstructuredWordDocumentLoader
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.schema import Document
from langchain.llms.openai import AzureOpenAI
from langchain.chains.qa_with_sources import load_qa_with_sources_chain
# from .constants import CSV_FILE_PATH
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity
from pathlib import Path

CSV_PATH = "data"
FILE_NAME = "output.csv"

# Construct the path to the file
CSV_FILE_PATH = Path(CSV_PATH) / FILE_NAME

logging.basicConfig(level=logging.ERROR)


'''Read PDF documents and return the list of langchain documents
'''
def readPDF(source_url):
    try:
        document_pages_lc = None
        document_pages_lc = PyPDFLoader(source_url).load()

        # for page in document_pages_lc:
            
        #     print(f'Source: {str(page.metadata["source"])}')
        #     print(f'Page: {str(int(page.metadata["page"])+1)}')
        #     print(page.page_content)

        return document_pages_lc
    except Exception as e:
        logging.error(f'Error readPDF(): {e}')
        return None

'''Read MS Word documents and return the list of langchain documents
'''
def readMSWord(source_url):
    try:
        one_page_size = 300 #IMP: How many words per split page of whole doc.
        document_pages_lc = None
        document_pages_lc = UnstructuredWordDocumentLoader(source_url).load() #Note: This method does not return same object as PDf loader, e.g. Doc pages not recognized. So below custom logic is built.
        document_pages_lc_list = []        
        
        # UnstructuredWordDocumentLoader returns whole doc as a single page, so need to impelement custom splitting
        for page in document_pages_lc:                       
            
            page_words = page.page_content.split(' ') #Split doc into words

            #Split document into pages of one_page_size words each
            for i in range((len(page_words) // one_page_size)+1):
                # print(i)
                
                # Note: Replaced below with Document object as in code below this section.
                # document_pages_lc_dict = {} #{"page_content":"",metadata={"source": "..doc", "page": 4}}
                # document_pages_lc_dict["page_content"] =  ' '.join(page_words[i*one_page_size:(i+1)*one_page_size])
                # document_pages_lc_dict["metadata"] = {"source":page.metadata["source"], "page":i}
                # document_pages_lc_list.append(document_pages_lc_dict)     

                doc = Document(page_content=' '.join(page_words[i*one_page_size:(i+1)*one_page_size]),
                               metadata={"source":page.metadata["source"], "page":i})
                document_pages_lc_list.append(doc)                   
        
        return document_pages_lc_list
    except Exception as e:
        logging.error(f'Error readMSWord_old(): {e}')
        return None
    
'''
Initialise environment variables
'''
def setEnv():
    try:
        openai.api_type = os.getenv('OPENAI_API_TYPE')
        openai.api_base = os.getenv('OPENAI_API_BASE')
        openai.api_version = os.getenv('API_VERSION')
        openai.api_key = os.getenv("OPENAI_API_KEY")
        
        return True
    except Exception as e:
        logging.error(f'Error setEnv(): {e}')    
        return False

'''
input_text: input text
'''
def encode(input_text):
    return str(hashlib.sha1(f'{input_text}'.encode('utf-8')).hexdigest())

'''
txt_data: input data
aoai_embedding_model: Azure OpenAI deployment name
chunk_size: Maximum number of texts to embed in each batch
max_retries: Maximum number of retries to make when generating.
'''
def getEmbedding(txt_data, aoai_embedding_model, chunk_size=1, max_retries = 3):
    try:
        embeddings = OpenAIEmbeddings(model=aoai_embedding_model, chunk_size=chunk_size, max_retries=max_retries)        
        query_result = embeddings.embed_query(txt_data)
        return query_result
    except Exception as e:
        logging.info(f'txt_data: {txt_data}')
        logging.error(f'Error getEmbedding(): {e}')        
        return None


'''
documentPath: Path to document (pdf/word/etc.)
'''
def getDocumentExtension(documentPath):
    try:
        return os.path.basename(documentPath).split('.')[len(os.path.basename(documentPath).split('.'))-1]
    except Exception as e:
        logging.error(f'Error getDocumentExtension(): {e}')    
        return None

'''
Removes new line characters, double spaces
input_text: Piece of text
'''
def cleanseText(input_text):
    try:
        input_text_cleansed = None
        input_text_cleansed = input_text.replace('\n',' ') #Remove new line characters
        input_text_cleansed = input_text_cleansed.replace('  ',' ') #Remove double space

        return input_text_cleansed
    except Exception as e:
        logging.error(f'Error cleanseText(): {e}')
        return None

'''
Generate embedding for entire doc
documentPath: Path to the document
'''
def getEmbeddingEntireDoc(documentPath, aoai_embedding_model, chunk_size=1):

    try:
        docType = None
        document_pages_lc = None
        document_page_embedding_list = []    
        document_page_content_list = []
        document_page_no_list = []

        #Get document type
        docType = getDocumentExtension(documentPath).lower()
        
        if docType == 'pdf':
            document_pages_lc = readPDF(documentPath)

        # Custom word doc processing as there's not page metadata like PDF loader, 
        # also the doc is not split into pages like PDF does out of the box. Please review readMSWord() method for more details.
        elif docType == 'docx' or docType == 'doc':
            document_pages_lc = readMSWord(documentPath)
        
        for document_page in document_pages_lc:
            # print(document_page)
            # print(document_page.page_content)
            # print(document_page.metadata["source"])
            # print(document_page.metadata["page"])

            source_doc_path = None
            source_doc_page_no = None
            source_doc_page_content = None
            embedding_result = None

            # if docType == 'pdf':
            #     source_doc_path = document_page.metadata["source"]
            #     source_doc_page_no = int(document_page.metadata["page"])
            #     source_doc_page_content = document_page.page_content

            # elif docType == 'docx' or docType == 'doc':
            #     source_doc_path = document_page["metadata"]["source"]
            #     source_doc_page_no = int(document_page["metadata"]["page"])
            #     source_doc_page_content = document_page["page_content"]

            source_doc_path = document_page.metadata["source"]
            source_doc_page_no = int(document_page.metadata["page"])
            source_doc_page_content = document_page.page_content
            
            # print(source_doc_path)
            # print(source_doc_page_no)
            # print(source_doc_page_content)

            source_doc_page_content_cleansed = cleanseText(source_doc_page_content)

            if (source_doc_page_content_cleansed) is not None and (len(source_doc_page_content_cleansed)>0) and (source_doc_page_content_cleansed.strip != ''):                    

                embedding_result = getEmbedding(source_doc_page_content_cleansed, aoai_embedding_model, chunk_size=1, max_retries = 3)
                # print(embedding_result)

                if embedding_result is not None:
                    document_page_content_list.append(source_doc_page_content) #Retain formatting
                    document_page_embedding_list.append(embedding_result)        
                    document_page_no_list.append(source_doc_page_no)
                else:
                    print(f'Unable to embed text:{source_doc_page_content}, moving to next.')

        return document_page_content_list, document_page_embedding_list, document_page_no_list
    except Exception as e:
        logging.error(f'Error getEmbeddingEntireDoc(): {e}')
        return None, None, None        



def create_csv(page_content, page_content_vector, page_number, documentPath, prefix = 'doc'):
    df = pd.DataFrame()
    
    # Super Important to include dtype parameter. Otherwise the record gets added but not seen by index!!!
    page_content_vector = np.array(page_content_vector, dtype=np.float32)        
    # print(f'page_content_vector.shape:{page_content_vector.shape}')       
    
    new_row = {'page_content': str(page_content), 'page_number': str(page_number),
            'document_path': str(documentPath), 'page_content_vector': page_content_vector.tobytes()}
    df = df.append(new_row, ignore_index=True)

    print(df)
    
    return df.to_csv(CSV_FILE_PATH, index=False, header=None, encoding='utf-8-sig')



def add_document_to_csv(documentPath, document_page_content_list, document_page_embedding_list, document_page_no_list, prefix, encrypt_prefix=False):
    try:
        if encrypt_prefix:            
            prefix = encode(prefix)
            
        # Iterate through pages
        for i, embedding in enumerate(document_page_embedding_list):   
            create_csv(page_content = document_page_content_list[i], 
                       page_content_vector = document_page_embedding_list[i], 
                       page_number = document_page_no_list[i], 
                       prefix = prefix,
                       documentPath = documentPath
            )

                        
        return True
    except Exception as e:
        logging.error(f'Error addDocumentToRedis(): {e}')
        return False



def query_csv(prompt, aoai_embedding_model, index_name, top_n, encrypt_index_name=False):
    try:
        document_lc_list = []
        df = pd.read_csv(CSV_FILE_PATH)

        if encrypt_index_name:
            index_name = encode(index_name)

        vec_prompt = getEmbedding(txt_data=prompt, aoai_embedding_model=aoai_embedding_model, chunk_size=1, max_retries = 3)
        vec_prompt = np.array(vec_prompt, dtype=np.float32)  #Super important to specify dtype, otherwise vector share mismatch error.

        # Convert the page_content_vector column to numpy arrays
        df["page_content_vector"] = df["page_content_vector"].apply(lambda x: np.frombuffer(x))


        # Compute cosine similarity between prompt vector and each row in DataFrame
        cosine_similarities = cosine_similarity(np.stack(df["page_content_vector"].values), [vec_prompt])

        # Add cosine similarities as a new column in DataFrame
        df["cosine_similarity"] = cosine_similarities

        # Sort DataFrame by cosine similarity in descending order
        df_sorted = df.sort_values(by="cosine_similarity", ascending=False)

        # Print the top 10 most similar rows
        print(df_sorted.head(10))
        
        # TODO: CHECK types
        # query_result = az_redis_connection.ft(index_name).search(query, {"prompt_vector": vec_prompt.tobytes()})          
        query_result = df_sorted.to_dict()

        #Create lc document, for use with lc
        for item in query_result.docs:
            document_lc = Document(page_content=item.page_content,metadata={"source":item.document_path, "page":item.page_number, "similarity":1-float(item.__page_content_vector_score)})
            document_lc_list.append(document_lc)

        return query_result, document_lc_list

    except Exception as e:
        logging.error(f'Error query_csv(): {e}')
        return None, None


#For cmd background colour
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
#-----------------------------------

aoai_embedding_models = {

    "text-search-ada-doc-001":{
        "version":{
            "1":{
                "deployment_name": "text-search-ada-doc-001-v1",
                "dim": 1024    
                }
            }    
        },

    "text-search-babbage-doc-001":{
        "version":{
            "1":{
                "deployment_name": "text-search-babbage-doc-001-v1",
                "dim": 2048    
                }
            }    
        },

    "text-search-curie-doc-001":{
        "version":{
            "1":{
                "deployment_name": "text-search-curie-doc-001-v1",
                "dim": 4096    
                }
            }    
        },

    "text-search-davinci-doc-001":{
        "version":{
            "1":{
                "deployment_name": "text-search-davinci-doc-001-v1",
                "dim": 12288    
                }
            }    
        },

    "text-embedding-ada-002":{
        "version":{
            "1":{
                "deployment_name": "text-embedding-ada-002-v1",
                "dim": 1536    
                }
            }    
        },

    "text-davinci-003":{
        "version":{
            "1":{
                "deployment_name": "text-davinci-003-v1"                
                }
            }    
        }

    }