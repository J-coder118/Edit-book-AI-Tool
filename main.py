import psycopg2
import json
import numpy as np
import faiss
import openai
import re
from sentence_transformers import SentenceTransformer
from flask import Flask, request, jsonify
from sqlalchemy import text
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import (
    create_engine,
    MetaData,
    inspect,
    Table,
    Column,
    Integer,
    String,
    Boolean,
    ARRAY,
    Float,
    Sequence,
    JSON,
)
from sqlalchemy.exc import SQLAlchemyError
from database_manager import DatabaseManager
from flask_jwt_extended import (
    jwt_required,
    verify_jwt_in_request,
    get_jwt_identity,
    JWTManager,
)
from flask_cors import CORS, cross_origin
from functools import wraps
import datetime
import logging

# Setup logging
logging.basicConfig(filename='logs/page_similarity_v4.4.log', level=logging.DEBUG, format='%(asctime)s [%(levelname)s] - %(message)s')
logger = logging.getLogger()


def jwt_required_except_options(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if request.method != "OPTIONS":
            try:
                verify_jwt_in_request()
            except Exception as e:
                logging.error(f"Error when verifying JWT token: {e}")
                return (
                    jsonify({"message": "Error when verifying JWT token"}),
                    401,
                )  # Unauthorized
        return fn(*args, **kwargs)

    return wrapper


app = Flask(__name__)
jwt = JWTManager(app)

app.config["JWT_SECRET_KEY"] = "1234"  # Change this to the actual JWT secret key
app.config["JWT_HEADER_NAME"] = "Authorization"
app.config["JWT_HEADER_TYPE"] = "Bearer"
app.config["JWT_TOKEN_LOCATION"] = ["headers"]
app.debug = True

CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)


db_manager = DatabaseManager(
    username="postgres",
    password="Kwantx11!!",
    host="internaldb.c0rz9kyyn4jp.us-east-2.rds.amazonaws.com",
    db_name="internaldb",
)

encoder = SentenceTransformer("distilbert-base-nli-mean-tokens")
index = faiss.IndexFlatL2(768)

# parsed_pages = Table("parsed_pages_2", db_manager.meta, autoload_with=db_manager.engine)
user_query = Table("user_query", db_manager.meta, autoload_with=db_manager.engine)

# Add OpenAI configurations
openai.api_key = "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"
model_engine = "gpt-4"


def chat_with_ai(user_input):
    conversation_history = [
        {
            "role": "system",
            "content": "You are an AI assistant that provides helpful answers.",
        },
        {"role": "user", "content": user_input},
    ]

    response = openai.ChatCompletion.create(
        model=model_engine,
        messages=conversation_history,
        max_tokens=2000,
        n=1,
        stop=None,
        temperature=0.7,
    )

    ai_response = response.choices[0].message["content"]
    print("OpenAI API Response:", response)
    app.logger.info("OpenAI API Response: %s", response)

    return ai_response


@app.route("/selected_papers", methods=["POST"])
# @cross_origin()
# @jwt_required_except_options
def handle_selected_papers():
    app.logger.info("Processing /selected_papers request...")
    start_time = datetime.datetime.now()
    # token = request.headers.get("Authorization")
    # print("Token in server: ", token)
    # logger.info("Token in server: %s", token)

    # if request.method != "OPTIONS":
    #     current_user = get_jwt_identity()
    #     if current_user is None:
    #         return (
    #             jsonify({"message": "Error: You are not authorized."}),
    #             401,
    #         )  # Unauthorized
    # else:
    #     current_user = None

    data = request.get_json()
    print("Inside /selected_papers route...")
    logger.info("Inside /selected_papers route...")
    print(f"Data received: {data}")
    logger.info("Data received: %s", data)

    user_id, query, selected_paper_ids = data.get('user_id'), data.get('query'), data.get('selected_paper_ids')

    if None in (user_id, query, selected_paper_ids):
        return (
            jsonify({"message": "Error: Missing required parameters."}),
            400,
        )  # Bad Request

    print(
        f"User ID: {user_id}, Query: {query}, Selected Paper IDs: {selected_paper_ids}"
    )
    logger.info(
        "User ID: %s, Query: %s, Selected Paper IDs: %s",
        user_id,
        query,
        selected_paper_ids,
    )

    k = 10  # number of closest pages to return

    print("Fetching user query embeddings...")
    logger.info("Fetching user query embeddings...")
    query_embeddings = []
    with db_manager.conn.begin():
        result = db_manager.conn.execute(
            text(
                f"SELECT query_embeddings FROM user_query WHERE user_id = '{user_id}' AND query = '{query}'"
            )
        )
            
        for row in result:
            query_embeddings = row[0]
    if query_embeddings == []:
        return jsonify({"failed": "query doesn't exist in user_query table. Please run abstract_test_v2.py file first."}), 404

    _vector = np.array(query_embeddings, dtype=np.float32).reshape(1, -1)
    faiss.normalize_L2(_vector)

    print("Fetching page embeddings for selected papers and building the index...")
    logger.info(
        "Fetching page embeddings for selected papers and building the index..."
    )
    parsed_pages = Table(
        "parsed_pages_2", db_manager.meta, autoload_with=db_manager.engine
    )
    page_id_to_page_info = {}
    idx_to_page_id = {}
    idx = 0
    with db_manager.conn.begin():
        for paper_id in selected_paper_ids:
            result = db_manager.conn.execute(
                text(
                    f"SELECT id, page_embeddings, page_number FROM parsed_pages_2 WHERE paper_id = {paper_id}"
                )
            )
            for row in result:
                id, embeddings_list, page_number = row
                embeddings = np.array(embeddings_list).reshape(1, -1)
                index.add(embeddings)
                idx_to_page_id[idx] = id
                page_id_to_page_info[id] = {
                    'paper_id': paper_id,
                    'page_number': page_number
                }
                idx += 1

    if idx_to_page_id == {}:
        return jsonify({"failed": "paper_id doesn't exist in parsed_pages_2 table."}), 404

    print("Searching the index...")
    logger.info("Searching the index...")
    D, I = index.search(_vector, k)
    closest_page_ids = list(set([idx_to_page_id[i] for i in I[0]]))
    print(f"Closest page IDs: {closest_page_ids}")
    logger.info("Closest page IDs: %s", closest_page_ids)

    print("Closest page IDs have been found. Proceeding with feature extraction...")
    logger.info(
        "Closest page IDs have been found. Proceeding with feature extraction..."
    )

    # Get the contents of these pages and extract the features
    features = []
    page_features = Table(
        "page_features", db_manager.meta, autoload_with=db_manager.engine
    )
    try:
        with db_manager.conn.begin():
            for page_id in closest_page_ids:
                result = db_manager.conn.execute(
                    text(
                        f"SELECT content FROM parsed_pages_2 WHERE id = '{page_id}'"
                    )
                )
                for row in result:
                    content_text = row[0][:1000]  # limit to 1000 characters

                    prompt = f"""
                    As an AI, I'm instructed to parse and understand the information from a technical paper and provide information about various features in a specific format. For each feature, I need to provide:

                    1A) Feature Name: The name of the feature
                    1B) Feature Formula: The mathematical formula for the feature
                    1C) Feature Definition: A brief description or definition of the feature
                    1D) Feature Parameters: Parameters or variables used in the feature formula as json format

                    Now, I'm reading this page:

                    {content_text}

                    Please provide the feature information according to the above format.
                    """

                    ai_chat = chat_with_ai(prompt)

                    # Split the ai_chat into individual feature information
                    # features_info = ai_chat.split("\n\n")
                    # for feature_info in features_info:
                    #     feature_name = re.search("1A\)(.*?);", feature_info)
                    #     feature_formula = re.search("1B\)(.*?);", feature_info)
                    #     feature_definition = re.search("1C\)(.*?);", feature_info)
                    #     feature_parameters = re.search("1D\)(.*?);", feature_info)
                    #     logger.info(f'feature_name: {feature_name}, feature_formula: {feature_formula}, , feature_definition: {feature_definition}')
                    #     if (
                    #         not feature_name
                    #         or not feature_formula
                    #         or not feature_definition
                    #         or not feature_parameters
                    #     ):
                    #         continue

                    #     feature_name = feature_name.group(1).strip()
                    #     feature_formula = feature_formula.group(1).strip()
                    #     feature_definition = feature_definition.group(1).strip()
                    #     feature_parameters = feature_parameters.group(1).strip()

                    # Regular expression pattern to match all the feature details
                    pattern = re.compile(r'\dA\) Feature Name: (.*?)\n'
                                        r'\dB\) Feature Formula: (.*?)\n'
                                        r'\dC\) Feature Definition: (.*?)\n'
                                        r'\dD\) Feature Parameters: (.*?)\n\n', re.DOTALL)

                    features_info = pattern.findall(ai_chat)

                    for feature in features_info:
                        try:
                            feature_name, feature_formula, feature_definition, feature_parameters = feature
                        except:
                            continue
                        logger.info(f'feature_name: {feature_name}, feature_formula: {feature_formula},  feature_definition: {feature_definition}, feature_parameters: {feature_parameters}')

                        if not all([feature_name, feature_formula, feature_definition, feature_parameters]):
                            continue

                        try:
                            decoded_parameters = json.loads(feature_parameters)
                            final_feature_parameters = decoded_parameters
                        except json.JSONDecodeError:
                            final_feature_parameters = {}

                        # Prepare a feature dictionary
                        feature = {
                            "user_id": user_id,
                            # "paper_id_page_number": page_id,
                            "paper_id": page_id_to_page_info[page_id]['paper_id'],
                            "page_number": page_id_to_page_info[page_id]['page_number'],
                            "feature_name": feature_name,
                            "feature_definition": feature_definition,
                            "feature_formula": feature_formula,
                            "feature_parameters": final_feature_parameters
                        }

                        logger.info(f' ====== feature:  {feature}')

                        features.append(feature)
                        
                        # Inserting into 'page_features' table
                        ins = page_features.insert().values(**feature)
                        db_manager.conn.execute(ins)
    except Exception as err:
        print(err)

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(f"Elapsed time for /selected_papers route: {elapsed_time}")
    logger.info("Elapsed time for /selected_papers route: %s", elapsed_time)

    return jsonify(features), 200  # OK


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5010)
