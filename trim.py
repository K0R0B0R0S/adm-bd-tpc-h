from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['tpc-h-array']


def trim_strings_in_document(document):
    if isinstance(document, dict):
        for key, value in document.items():
            document[key] = trim_strings_in_document(value)
    elif isinstance(document, list):
        document = [trim_strings_in_document(item) for item in document]
    elif isinstance(document, str):
        document = document.strip()
    return document

for collection_name in db.list_collection_names():
    collection = db[collection_name]
    documents = collection.find()

    print(f"Processing collection {collection_name}...")

    for index, document in enumerate(documents):
        trimmed_document = trim_strings_in_document(document)
        collection.update_one({'_id': document['_id']}, {'$set': trimmed_document})
        if index % 1000 == 0:
            print(f"{index} documents processed")

