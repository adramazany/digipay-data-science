from elasticsearch import Elasticsearch
from deepface.basemodels import Facenet
import os
from deepface.commons import functions
import matplotlib.pyplot as plt
from digipay import config

#########################################################


#########################################################
def create_elastic_index(es,embedding_size):
    mapping = {
        "mappings": {
            "properties": {
                "title_vector":{
                    "type": "dense_vector",
                    "dims": embedding_size
                },
                "title_name": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index="face_recognition", body=mapping)


#########################################################
def load_files(face_dataset_path):
    files = []
    for r, d, f in os.walk(face_dataset_path):
        for file in f:
            if ('.jpg' in file):
                exact_path = r + file
                files.append(exact_path)
    print('load_files=',files)
    return files


#########################################################
def reconize_store_face(es,files,target_size):
    index = 0
    for img_path in files:
        #print(img_path)
        img = functions.preprocess_face(img_path, target_size = target_size)
        embedding = model.predict(img)[0]

        doc = {"title_vector": embedding, "title_name": img_path}
        es.create("face_recognition", id=index, body=doc)

        index = index + 1


#########################################################
def recognize_face(target_path,model,target_size):
    # target_path = "target.jpg"
    target_img = functions.preprocess_face(target_path, target_size = target_size)
    target_embedding = model.predict(target_img)[0]
    return target_embedding

#########################################################
def search_face(es,target_embedding):
    query = {
        "size": 5,
        "query": {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    #"source": "cosineSimilarity(params.queryVector, 'title_vector') + 1.0",
                    "source": "1 / (1 + l2norm(params.queryVector, 'title_vector'))", #euclidean distance
                    "params": {
                        "queryVector": list(target_embedding)
                    }
                }
            }
        }}
    res = es.search(index="face_recognition", body=query)
    return res

#########################################################
def plot_search_face_result(target_path,res):
    target_img = functions.preprocess_face(target_path, target_size = target_size)
    for i in res["hits"]["hits"]:
        candidate_name = i["_source"]["title_name"]
        candidate_score = i["_score"]
        print(candidate_name, ": ", candidate_score)

        candidate_img = functions.preprocess_face(candidate_name)[0]

        fig = plt.figure()

        ax1 = fig.add_subplot(1, 2, 1)
        plt.imshow(target_img[0][:,:,::-1])
        plt.axis('off')

        ax2 = fig.add_subplot(1, 2, 2)
        plt.imshow(candidate_img[:,:,::-1])
        plt.axis('off')

        plt.show()

        print("-------------------------")


#########################################################

# load face model
model = Facenet.loadModel()
target_size = (160, 160)
embedding_size = 128

# es = Elasticsearch([{'host': 'localhost', 'port': '9200'}],http_auth=(config.ES_USER,config.ES_PASS))
es = Elasticsearch([{'host': '172.18.24.85', 'port': '9200'}])local-H2-gl-runtime

# EXEC_TYPE='CREATE-INDEX'
# EXEC_TYPE='RECOGNIZE'
EXEC_TYPE='SEARCH'

if EXEC_TYPE=='CREATE-INDEX':
    create_elastic_index(es,embedding_size)

if EXEC_TYPE=='RECOGNIZE':
    files = load_files("/Users/adel/java/ds-workspace/digipay-data-science/py-mongo-data-eval/face/dataset/")
    reconize_store_face(es,files,target_size)
    print("RECOGNIZE succed.")

if EXEC_TYPE=='SEARCH':
    target_path = "/Users/adel/java/ds-workspace/digipay-data-science/py-mongo-data-eval/face/target.jpg"
    target_embedding = recognize_face(target_path , model , target_size)
    res = search_face(es,target_embedding)
    plot_search_face_result(target_path,res)
    print("SEARCH succed.")

print("finished.")

