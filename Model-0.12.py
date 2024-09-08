#~~~pip install pandas numpy seaborn matplotlib scikit-learn xgboost confluent-kafka orjson lightgbm catboost pyarrow~~~#
#########################################################################################################################
# Imports:
import pandas as pd
from confluent_kafka import Consumer, KafkaError
import json
from sklearn.preprocessing import StandardScaler, OneHotEncoder, label_binarize
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import confusion_matrix
from lightgbm import LGBMClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from xgboost import XGBClassifier
from sklearn.metrics import (classification_report, roc_auc_score, average_precision_score, fbeta_score, cohen_kappa_score, matthews_corrcoef, log_loss)
from scipy.sparse import csr_matrix
import joblib
import numpy as np
import logging
import cuml
import xgboost as xgb
from cuml.ensemble import RandomForestClassifier as cuRF
from cuml.neighbors import KNeighborsClassifier as cuKNN
from cuml.linear_model import LogisticRegression as cuLR
from catboost import CatBoostClassifier
import pyarrow as pa
import pyarrow.parquet as pq
import os
import dask.dataframe as dd
import time
import psutil
import gc
import torch  # For CUDA memory clearing if applicable

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("training_pipeline.log"),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# Kafka Consumer setup
def setup_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9095',
        'group.id': 'FastAsFuckBoii',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe(['elastiflow-flow-codex-6-1.0'])
    return consumer

def process_message(msg):
    netflow_data = json.loads(msg.value().decode('utf-8'))
    return {
        "src_mac": netflow_data.get("flow.in.src.mac.addr"),
        "dst_mac": netflow_data.get("flow.in.dst.mac.addr"),
        "bytes": netflow_data.get("flow.bytes"),
        "packets": netflow_data.get("flow.packets"),
        "direction": netflow_data.get("flow.direction.name"),
        "protocol": netflow_data.get("l4.proto.name"),
        "client_as_label": netflow_data.get("flow.client.as.label"),
        "server_as_label": netflow_data.get("flow.server.as.label"),
        "client_as_org": netflow_data.get("flow.client.as.org"),
        "server_as_org": netflow_data.get("flow.server.as.org"),
        "start_offset": netflow_data.get("flow.start.time_offset"),
        "in_bytes": netflow_data.get("flow.in.bytes"),
        "ttl": netflow_data.get("ip.ttl"),
        "flow_export_timestamp": netflow_data.get("flow.export.timestamp"),
        "server_asn": netflow_data.get("flow.server.as.asn"), 
        "client_asn": netflow_data.get("flow.client.as.asn"), 
        "client_port": netflow_data.get("flow.client.l4.port.id"),
        "server_port": netflow_data.get("flow.server.l4.port.id")
    }

def consume_and_store_data_to_parquet(consumer, output_file_prefix, message_limit=8000000):
    """
    Consume Kafka data and store it in parquet files in batches.
    """
    batch_size = 8000000
    captured_data = []  # Initialize captured_data within the function
    data = []
    total_data = 0
    batch_num = 1  # Initialize batch number counter
    
    while len(captured_data) < message_limit:
        msgs = consumer.consume(timeout=0.1)
        for msg in msgs:
            if msg.error():
                logger.error(f"Message consumption error: {msg.error()}")
                continue
            result = process_message(msg)
            if result:
                captured_data.append(result)  # Accumulate messages in captured_data
                data.append(result)
        
        if len(data) >= batch_size:  # Write to parquet file after every batch
            output_file = f"{output_file_prefix}_batch_{batch_num}.parquet"
            df = pd.DataFrame(data)
            df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
            data = []  # Reset the data batch after writing
            total_data += batch_size
            logger.info(f"{total_data} messages written to {output_file} so far.")
            batch_num += 1  # Increment the batch number

        if len(captured_data) >= message_limit:
            break

    # Write remaining data if any before closing the consumer
    if data:
        output_file = f"{output_file_prefix}_batch_{batch_num}.parquet"
        df = pd.DataFrame(data)
        df.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
        total_data += len(data)
        logger.info(f"{total_data} messages written to {output_file} so far.")
        batch_num += 1

    consumer.close()

    logger.info(f"Captured {total_data} messages saved across batches.")
    return batch_num  # Return the total number of batches written



def replay_from_parquet_in_batches_pandas(file_path, batch_size):
    """
    Read the entire Parquet file using Pandas and yield data in batches.
    """
    df = pd.read_parquet(file_path, engine='pyarrow')
    num_rows = df.shape[0]

    # Split the data into chunks of batch_size
    for start in range(0, num_rows, batch_size):
        df_batch = df.iloc[start:start + batch_size]
        df_batch = preprocess_data(df_batch)
        df_batch = create_multiclass_labels(df_batch, mac_addresses)

        # Split into X and y
        X = apply_preprocessor(preprocessor, df_batch, fit=False)
        y = df_batch['label']

        yield X, y


def replay_from_parquet_in_batches_pandas(file_path, batch_size, preprocessor):
    """
    Read the entire Parquet file using Pandas and yield data in batches.
    """
    df = pd.read_parquet(file_path, engine='pyarrow')
    num_rows = df.shape[0]

    # Split the data into chunks of batch_size
    for start in range(0, num_rows, batch_size):
        df_batch = df.iloc[start:start + batch_size]
        df_batch = preprocess_data(df_batch)
        
        # Ensure labels are created for the current batch
        df_batch = create_multiclass_labels(df_batch, mac_addresses)

        # Apply the fitted preprocessor to the batch
        X = apply_preprocessor(preprocessor, df_batch, fit=False)
        y = df_batch['label']  # Now the 'label' column should exist

        yield X, y
        
def replay_from_parquet_in_batches_dask(file_path, batch_size, preprocessor):
    """
    Read the Parquet file in chunks using Dask and yield preprocessed batches.
    """
    df = dd.read_parquet(file_path, engine='pyarrow')

    # Split the data into chunks of batch_size
    for start in range(0, len(df), batch_size):
        df_batch = df.loc[start:start + batch_size].compute()  # Convert batch to pandas for processing
        df_batch = preprocess_data(df_batch)  # Preprocess batch
        
        # Ensure labels are created for the current batch
        df_batch = create_multiclass_labels(df_batch, mac_addresses)
        
        # Apply the fitted preprocessor to the batch
        X = apply_preprocessor(preprocessor, df_batch, fit=False)
        y = df_batch['label']  # Now the 'label' column should exist

        yield X, y


def preprocess_first_batch_dask(file_path, batch_size):
    """
    Preprocess the first batch of data using Dask and fit the preprocessor.
    """
    df = dd.read_parquet(file_path, engine='pyarrow')
    df_first_batch = df.loc[:batch_size].compute()  # Convert first batch to pandas for processing
    df_first_batch = preprocess_data(df_first_batch)  # Apply initial preprocessing

    return df_first_batch



def preprocess_data(df):
    """
    Preprocesses the dataframe by filling missing values separately for numeric and categorical columns.
    """
    # Fill missing values for numeric columns with 0
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df.loc[:, numeric_columns] = df[numeric_columns].fillna(0)

    # Fill missing values for categorical columns with empty strings or a placeholder
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns
    df.loc[:, categorical_columns] = df[categorical_columns].fillna('missing')

    df = df.infer_objects()  # Automatically convert types if needed
    
    categorical_columns = ['direction', 'protocol', 'client_as_label', 'server_as_label', 'client_as_org', 'server_as_org']
    for col in categorical_columns:
        df[col] = df[col].astype(str)

    # Extract hour from timestamp
    df['hour'] = pd.to_datetime(df['flow_export_timestamp'], unit='ms').dt.hour

    logger.info("Data preprocessing completed.")
    return df


def build_preprocessor_for_model(model_name, numerical_features, categorical_features):
    """
    Build the appropriate preprocessor based on the type of model.
    """
    if model_name in ['RF', 'CAT']:  # Tree-based models (RandomForest, CatBoost)
        # Tree-based models generally do not require scaling, but categorical features should be handled
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ]
        )
    elif model_name in ['XGB', 'LGBM']:  # XGBoost, LightGBM require categorical encoding but no scaling
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ]
        )
    elif model_name in ['LR', 'MLP', 'KNN', 'NB']:  # Models requiring scaling and encoding
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', StandardScaler(), numerical_features),
                ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
            ]
        )
    else:
        raise ValueError(f"Unsupported model type: {model_name}")

    return preprocessor

def apply_preprocessor(preprocessor, df, fit=False):
    """
    Apply the preprocessor to the dataframe.
    Set fit=True only for the first batch, as the preprocessor should be fitted once.
    """
    if fit:
        df_preprocessed = preprocessor.fit_transform(df)
    else:
        df_preprocessed = preprocessor.transform(df)
    
    if isinstance(df_preprocessed, csr_matrix):
        df_preprocessed = df_preprocessed.toarray()

    logger.info("Preprocessing applied to data.")
    return pd.DataFrame(df_preprocessed)

def create_multiclass_labels(df, mac_addresses):
    def assign_label(row):
        src_mac = row.get('src_mac')
        dst_mac = row.get('dst_mac')
    
        if pd.notna(src_mac) and pd.notna(dst_mac):
            for i, mac in enumerate(mac_addresses, 1):
                if src_mac == mac or dst_mac == mac:
                    return i
        return 0  # Assuming 0 for no match

    df['label'] = df.apply(assign_label, axis=1)
    return df

def clear_memory():
    """Clear memory by running garbage collector and clearing CUDA cache (if applicable)."""
    logger.info("Clearing memory before starting the next model...")

    # Clear Python objects and unreferenced memory
    gc.collect()

    # Clear GPU memory cache (for CUDA-based models)
    torch.cuda.empty_cache()  # Optional: If using CUDA, clear GPU memory

    memory_used = psutil.virtual_memory().used / (1024 ** 3)
    logger.info(f"Memory after clearing: {memory_used:.2f} GB")

def train_model_on_fly(models, model_names, file_path, batch_size=8000000, classes=None):
    for model, model_name in zip(models, model_names):
        clear_memory()
        logger.info(f"Preparing preprocessor and training for model: {model_name}")

        # Preprocess the first batch and fit the preprocessor
        df_first_batch = preprocess_first_batch_dask(file_path, batch_size)
        preprocessor = build_preprocessor_for_model(model_name, numerical_features, categorical_features)
        df_preprocessed = apply_preprocessor(preprocessor, df_first_batch, fit=True)

        joblib.dump(preprocessor, f'preprocessor_{model_name}.pkl')
        logger.info(f"Preprocessor fitted and saved as preprocessor_{model_name}.pkl")

        start_time = time.time()
        logger.info(f"Training model: {model_name}")
        memory_before = psutil.virtual_memory().used

        total_processed_samples = 0
        y_true_all = []
        y_pred_all = []
        y_pred_prob_all = []

        # Train on batches
        for X_batch, y_batch in replay_from_parquet_in_batches_dask(file_path, batch_size, preprocessor):
            total_processed_samples += len(X_batch)

            # Split data into training and validation sets
            X_train, X_val, y_train, y_val = train_test_split(X_batch, y_batch, test_size=0.2, random_state=42)

            if model_name == 'XGB':
                eval_set = [(X_train, y_train), (X_val, y_val)]  # Evaluation set
                model.set_params(eval_metric="mlogloss")
                model.fit(X_train, y_train, eval_set=eval_set, verbose=True)
            else:
                if hasattr(model, 'partial_fit'):
                    model.partial_fit(X_train, y_train, classes=classes)
                else:
                    model.fit(X_train, y_train)

            y_pred = model.predict(X_val)
            y_true_all.extend(y_val)
            y_pred_all.extend(y_pred)

            if hasattr(model, 'predict_proba'):
                y_pred_prob = model.predict_proba(X_val)
                if len(y_pred_prob_all) == 0:
                    y_pred_prob_all = y_pred_prob
                else:
                    y_pred_prob_all = np.vstack((y_pred_prob_all, y_pred_prob))

            logger.info(f"Processed batch with {len(X_batch)} samples for model: {model_name}")

        # After training on all batches, call evaluate_model
        evaluate_model(y_true_all, y_pred_all, y_pred_prob_all, model_name)

        memory_after = psutil.virtual_memory().used
        logger.info(f"Memory used by {model_name}: {(memory_after - memory_before) / (1024 ** 3):.2f} GB")
        joblib.dump(model, f'device_detection_model_{model_name}.pkl')
        logger.info(f"Model {model_name} saved successfully.")
        logger.info(f"Training time for model {model_name}: {time.time() - start_time:.2f} seconds")


def evaluate_model(y_true_all, y_pred_all, y_pred_prob_all, model_name):
    """Evaluate the trained model using different metrics"""
    y_true_bin = label_binarize(y_true_all, classes=np.unique(y_true_all))

    if len(np.unique(y_true_all)) > 1 and y_pred_prob_all is not None:
        roc_auc = roc_auc_score(y_true_bin, np.array(y_pred_prob_all), average='macro', multi_class='ovr')
        logger.info(f"AUC-ROC for {model_name}: {roc_auc:.4f}")

        pr_auc = average_precision_score(y_true_bin, np.array(y_pred_prob_all), average='macro')
        logger.info(f"PR AUC for {model_name}: {pr_auc:.4f}")

    cm = confusion_matrix(y_true_all, y_pred_all)
    logger.info(f"Confusion Matrix for {model_name}:\n{cm}")

    report = classification_report(y_true_all, y_pred_all, output_dict=False)
    logger.info(f"Classification Report for {model_name}:\n{report}")

    summary_report = classification_report(y_true_all, y_pred_all, output_dict=True)
    precision = summary_report['weighted avg']['precision']
    recall = summary_report['weighted avg']['recall']
    f1_score = summary_report['weighted avg']['f1-score']
    accuracy = summary_report['accuracy']

    logger.info(f"Compact Summary for {model_name}:\n"
                f"Accuracy: {accuracy:.4f}\n"
                f"Precision: {precision:.4f}\n"
                f"Recall: {recall:.4f}\n"
                f"F1-Score: {f1_score:.4f}")

    f2_score = fbeta_score(y_true_all, y_pred_all, beta=2, average='weighted')
    logger.info(f"F2-Score for {model_name}: {f2_score:.4f}")

    kappa = cohen_kappa_score(y_true_all, y_pred_all)
    logger.info(f"Cohen's Kappa for {model_name}: {kappa:.4f}")

    mcc = matthews_corrcoef(y_true_all, y_pred_all)
    logger.info(f"Matthews Correlation Coefficient (MCC) for {model_name}: {mcc:.4f}")

    if y_pred_prob_all is not None:
        logloss = log_loss(y_true_all, np.array(y_pred_prob_all))
        logger.info(f"Log Loss for {model_name}: {logloss:.4f}")

    class_accuracies = []
    unique_classes = np.unique(y_true_all)

    for cls in unique_classes:
        correct_predictions = sum(
            (true == pred) for true, pred in zip(np.array(y_true_all), np.array(y_pred_all)) if true == cls
        )
        total_instances = sum(1 for true in np.array(y_true_all) if true == cls)
        class_acc = correct_predictions / total_instances if total_instances > 0 else 0
        class_accuracies.append(class_acc)

    mean_class_accuracy = np.mean(class_accuracies)
    logger.info(f"Mean Class Accuracy for {model_name}: {mean_class_accuracy:.4f}")

    top4_acc = top_n_accuracy(y_true_all, y_pred_prob_all, 4)
    logger.info(f"Top-4 Accuracy for {model_name}: {top4_acc:.4f}")


        
def top_n_accuracy(y_true, y_pred_prob, n):
    top_n_preds = np.argsort(y_pred_prob, axis=1)[:, -n:]  # Get the top N predicted classes
    hits = [1 if y in preds else 0 for y, preds in zip(y_true, top_n_preds)]
    return np.mean(hits)



# Features for preprocessing
numerical_features = [
    'bytes', 'packets', 'server_asn', 'client_asn', 'server_port', 'client_port',
    'flow_export_timestamp', 'hour', 'in_bytes', 'ttl'
]
categorical_features = ['direction', 'protocol', 'client_as_label', 'server_as_label', 'client_as_org', 'server_as_org']

# MAC addresses
mac_addresses = ['00:00:00:00:00:01', '00:00:00:00:00:02', '00:00:00:00:00:03', '00:00:00:00:00:04', '00:00:00:00:00:05', '00:00:00:00:00:06', '00:00:00:00:00:07']

# Models and names
models = [
    cuRF(n_estimators=500, random_state=42),
    cuLR(max_iter=3000, random_state=42),
    XGBClassifier(n_estimators=500, use_label_encoder=False, eval_metric='logloss', random_state=42, device='cuda'),
    MLPClassifier(hidden_layer_sizes=(100,), max_iter=500, random_state=42),
    LGBMClassifier(n_estimators=200, random_state=42, device='cpu'),
    cuKNN(n_neighbors=50),
    GaussianNB(),
    CatBoostClassifier(iterations=1000, depth=10, learning_rate=0.1, task_type="GPU")
]
model_names = ['RF', 'LR', 'XGB', 'MLP', 'LGBM', 'KNN', 'NB', 'CAT']

# File to store the parquet data
parquet_file = 'netflow_data_combined.parquet'

# Setup Kafka consumer and store data to parquet file
consumer = setup_consumer()

# Check if the combined parquet file already exists
if os.path.exists(parquet_file):
    logger.info(f"Parquet file {parquet_file} already exists. Skipping data capture.")
else:
    # Consume and store Kafka data if parquet file does not exist
    output_file_prefix = 'netflow_data.parquet'
    batch_num = consume_and_store_data_to_parquet(consumer, output_file_prefix, message_limit=8000000)
    
    # Combine the batch files into one large Parquet file
    parquet_files = [f"{output_file_prefix}_batch_{i}.parquet" for i in range(1, batch_num)]
    tables = [pq.read_table(f) for f in parquet_files]
    combined_table = pa.concat_tables(tables)
    pq.write_table(combined_table, parquet_file)

# Build and apply the preprocessor for each model
for model, model_name in zip(models, model_names):
    preprocessor = build_preprocessor_for_model(model_name, numerical_features, categorical_features)

    logger.info(f"Loading data from {parquet_file} for model {model_name} and training.")
    train_model_on_fly([model], [model_name], parquet_file, batch_size=8000000, classes=np.unique([0, 1, 2, 3, 4, 5, 6, 7]))

logger.info("Training pipeline completed.")