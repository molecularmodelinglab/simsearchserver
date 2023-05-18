import requests
import yaml

def query_morgan_pca_16(embedding):

    port = 3000
    query_descriptor = embedding
    query_descriptor_string = ",".join([f"{x:.4f}" for x in query_descriptor])

    query_string = f"http://127.0.0.1:{port}/descriptor/10/{query_descriptor_string}"
    r = requests.get(query_string)
    s = r.content.decode()
    print(s)
    d = yaml.safe_load(s[:-1])

    print(d)
    return d



def query_smallsa_16():
    raise NotImplementedError

def query_smallsa_8():
    raise NotImplementedError

