# ML Runtime Migration Guide

This guide covers the specific changes when migrating between standard and ML runtimes across DBR 13.3 LTS and DBR 16.4 LTS, and from ML runtime to serverless compute.

---

## ML Runtime Overview

| Component | DBR 13.3 ML | DBR 16.4 ML | Serverless (env v4) |
|-----------|-------------|-------------|---------------------|
| Python | 3.10 | 3.12 | 3.12 |
| Spark | 3.4.1 | 3.5.x | Serverless engine |
| Scala | 2.12.15 | 2.12.18 | N/A |
| PyTorch | 1.13.1 | 2.1+ | Via requirements.txt |
| TensorFlow | 2.12 | 2.15+ | Via requirements.txt |
| scikit-learn | 1.1.x | 1.3+ | Via requirements.txt |
| XGBoost | 1.7.x | 2.0+ | Via requirements.txt |
| pandas | 1.5.x | 2.1+ | 2.x (pre-installed) |
| numpy | 1.23.x | 1.26+ | 1.26+ (pre-installed) |
| MLflow | 2.6.x | 2.11+ | Via requirements.txt |
| Hugging Face Transformers | 4.30.x | 4.36+ | Via requirements.txt |
| LightGBM | 3.3.x | 4.1+ | Via requirements.txt |

---

## Standard Runtime ↔ ML Runtime Considerations

### When Migrating FROM ML Runtime

If the source notebook runs on ML runtime but the migration target is standard runtime or serverless, check for ML runtime pre-installed packages that are NOT available on the target.

**Common ML packages that need explicit installation on standard/serverless:**

| Package | ML Runtime | Standard Runtime | Serverless |
|---------|-----------|-----------------|------------|
| `torch` / `pytorch` | Pre-installed | Not available | requirements.txt |
| `tensorflow` | Pre-installed | Not available | requirements.txt |
| `scikit-learn` | Pre-installed | Not available | requirements.txt |
| `xgboost` | Pre-installed | Not available | requirements.txt |
| `lightgbm` | Pre-installed | Not available | requirements.txt |
| `transformers` | Pre-installed | Not available | requirements.txt |
| `mlflow` | Pre-installed | Pre-installed | Pre-installed (check version) |
| `hyperopt` | Pre-installed | Not available | requirements.txt |
| `shap` | Pre-installed | Not available | requirements.txt |
| `horovod` | Pre-installed | Not available | **Not compatible** with serverless |
| `petastorm` | Pre-installed | Not available | requirements.txt |
| `spark-tensorflow-distributor` | Pre-installed | Not available | **Not compatible** with serverless |

### Detection: ML Runtime Dependencies

```regex
# ML framework imports
(?m)^(?:from|import)\s+(?:torch|tensorflow|tf|keras|sklearn|scikit_learn|xgboost|lightgbm|transformers|hyperopt|shap|horovod|petastorm)

# MLlib imports
(?m)^(?:from|import)\s+(?:pyspark\.ml|pyspark\.mllib)

# Spark ML functions
(?m)^(?:from|import)\s+(?:sparkdl|spark_tensorflow_distributor)

# Databricks AutoML
(?m)databricks\.automl
(?m)databricks\.feature_store
(?m)databricks\.feature_engineering
```

---

## Library-Specific Migration Notes

### scikit-learn 1.1.x → 1.3+

| Change | Impact | Action |
|--------|--------|--------|
| `sklearn.utils.validation.check_is_fitted` | Signature changed | Update function call |
| `n_features_` attribute | Renamed to `n_features_in_` | Update attribute access |
| `set_params` stricter validation | May reject previously accepted params | Validate param dicts |
| `FutureWarning` → errors | Deprecated features removed | Fix all FutureWarnings first |
| `sklearn.preprocessing.OrdinalEncoder` | `handle_unknown` param added | Update if using |

```python
# BEFORE (1.1.x):
from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier(n_estimators=100)
model.fit(X, y)
print(model.n_features_)  # Deprecated in 1.1, removed in 1.3

# AFTER (1.3+):
model.fit(X, y)
print(model.n_features_in_)  # New name
```

### XGBoost 1.7.x → 2.0+

| Change | Impact | Action |
|--------|--------|--------|
| `use_label_encoder` parameter | Removed (was deprecated) | Remove the parameter |
| Default `tree_method` | Changed to `hist` | Usually fine; remove explicit `tree_method="exact"` if set |
| `eval_metric` default | Changed for some objectives | Verify metric names |
| GPU support changes | Different API | Update if using GPU training |
| Spark integration | `xgboost.spark` is the new API | Update from `sparkxgb` |

```python
# BEFORE (1.7.x):
import xgboost as xgb
model = xgb.XGBClassifier(use_label_encoder=False, eval_metric="logloss")

# AFTER (2.0+):
model = xgb.XGBClassifier(eval_metric="logloss")
# use_label_encoder removed — just delete the parameter
```

### pandas 1.5.x → 2.x

| Change | Impact | Action |
|--------|--------|--------|
| `DataFrame.append()` | **Removed** | Use `pd.concat([df1, df2])` |
| `inplace=True` on many methods | Deprecated (still works but warns) | Remove `inplace=True`, use assignment |
| `DataFrame.swaplevel()` | Changed default `axis` | Specify axis explicitly |
| Copy-on-Write | Default in pandas 2.0 | Chained assignment `df["a"]["b"] = x` may not work |
| `datetime64[ns]` → `datetime64[s]` | Default precision changed | May affect timestamp comparisons |
| Nullable integer types | Default for some operations | Check int dtype handling |

```python
# BEFORE (1.5.x):
df = df.append(new_row, ignore_index=True)
df.drop(columns=["temp"], inplace=True)

# AFTER (2.x):
df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
df = df.drop(columns=["temp"])
```

### numpy 1.23.x → 1.26+

| Change | Impact | Action |
|--------|--------|--------|
| `np.bool` / `np.int` / `np.float` | **Removed** (aliases for Python builtins) | Use `np.bool_` / `np.int64` / `np.float64` |
| `np.str` / `np.object` | **Removed** | Use `np.str_` / `np.object_` |
| `np.complex` | **Removed** | Use `np.complex128` |
| Promotion rules | Stricter | May affect mixed-type operations |

```python
# BEFORE (1.23.x):
arr = np.array([1, 2, 3], dtype=np.int)  # Deprecated alias
mask = np.array([True, False], dtype=np.bool)

# AFTER (1.26+):
arr = np.array([1, 2, 3], dtype=np.int64)
mask = np.array([True, False], dtype=np.bool_)
```

### PyTorch 1.13.x → 2.1+

| Change | Impact | Action |
|--------|--------|--------|
| `torch.compile()` | New optimization | Available but not required |
| `torch.nn.Module.load_state_dict(strict=True)` | Stricter by default | Verify model loading |
| `torch.utils.data` | Some DataLoader changes | Check DataLoader params |
| CUDA compatibility | Different CUDA versions | Verify GPU driver compatibility |

**Note:** PyTorch is NOT compatible with serverless compute for training. Inference-only workloads may work via CPU with requirements.txt.

### TensorFlow 2.12 → 2.15+

| Change | Impact | Action |
|--------|--------|--------|
| `tf.compat.v1` | Further deprecated | Migrate to tf2 API |
| Keras integration | Standalone keras package | Update imports: `import keras` not `from tensorflow import keras` |
| `tf.function` | More aggressive tracing | May surface latent bugs |

**Note:** TensorFlow training requires GPU clusters, not compatible with serverless. Inference may work with CPU.

### MLflow 2.6.x → 2.11+

| Change | Impact | Action |
|--------|--------|--------|
| `mlflow.sklearn.log_model` | Signature options added | Update if using signatures |
| `mlflow.pyfunc` | Input type handling | Check model input types |
| Model Registry | Gateway changes | Use `models:/` URI format |
| MLflow Tracking | New experiment/run APIs | Minor API changes |

### Hugging Face Transformers 4.30.x → 4.36+

| Change | Impact | Action |
|--------|--------|--------|
| `AutoModelForCausalLM` | New model support | Compatible |
| `pipeline()` | New task types | Compatible |
| `tokenizer` | Some deprecated methods | Update if using deprecated APIs |
| Model caching | Changed default cache dir | Update if relying on specific cache paths |

---

## MLlib API Changes (13.3 → 16.4)

### Deprecated Features

| Feature | Status in 16.4 | Replacement |
|---------|---------------|-------------|
| `pyspark.mllib` (RDD-based ML) | Deprecated (still works) | Use `pyspark.ml` (DataFrame-based) |
| `StringIndexer` without `handleInvalid` | Default behavior changed | Set `handleInvalid="keep"` explicitly |
| `VectorAssembler` without `handleInvalid` | May error on nulls in ANSI mode | Set `handleInvalid="skip"` or `"keep"` |
| Feature Store Client | Deprecated | Use `databricks.feature_engineering` |

### ANSI Mode Impact on MLlib

```python
# BEFORE (ANSI off — nulls silently passed through):
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="category", outputCol="category_idx")

# AFTER (ANSI on — must handle invalid/null values explicitly):
indexer = StringIndexer(
    inputCol="category",
    outputCol="category_idx",
    handleInvalid="keep"  # or "skip" — prevents ANSI exceptions
)
```

### Feature Store → Feature Engineering

```python
# BEFORE (13.3 — Feature Store Client):
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
fs.create_table(name="catalog.schema.features", primary_keys=["id"], df=feature_df)

# AFTER (16.4 — Feature Engineering Client):
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
fe.create_table(name="catalog.schema.features", primary_keys=["id"], df=feature_df)
```

---

## Serverless Limitations for ML Workloads

### Not Supported on Serverless

| Feature | Status | Alternative |
|---------|--------|------------|
| GPU access | Not available | Use GPU job clusters |
| Distributed training (Horovod, TorchDistributor) | Not available | Use ML job clusters |
| Spark TensorFlow Distributor | Not available | Use ML job clusters |
| Custom Docker containers | Not available | Use job clusters with Docker |
| Large model loading (> driver memory) | Limited by serverless resources | Use model serving endpoints |

### Supported on Serverless

| Feature | Status | Notes |
|---------|--------|-------|
| MLlib (pyspark.ml) | Supported | DataFrame-based ML works |
| MLflow tracking and logging | Supported | Via pre-installed MLflow |
| Feature Engineering reads | Supported | Can read feature tables |
| Small model inference | Supported | Via pandas_udf or Python UDFs |
| scikit-learn (CPU) | Supported | Via requirements.txt |
| XGBoost (CPU) | Supported | Via requirements.txt |
| LightGBM (CPU) | Supported | Via requirements.txt |

### Recommendation

For ML notebooks being migrated to serverless:

1. **Data preparation notebooks** → Serverless (good fit)
2. **Feature engineering notebooks** → Serverless (good fit)
3. **Model training notebooks** → Keep on ML runtime clusters (not serverless)
4. **Model inference (batch scoring) notebooks** → Serverless if CPU-only, otherwise ML clusters
5. **Model evaluation notebooks** → Serverless (good fit)

---

## Detection: ML Runtime Only Features

When scanning notebooks for serverless eligibility, flag these as requiring ML runtime or dedicated clusters:

```regex
# GPU usage
(?i)\.cuda\(\)
(?i)torch\.device\s*\(\s*["']cuda
(?i)tensorflow.*GPU
(?i)with\s+tf\.device.*GPU
(?i)cudf
(?i)cuml

# Distributed training
(?i)horovod
(?i)TorchDistributor
(?i)spark_tensorflow_distributor
(?i)SparkTorchDistributor

# Large model operations
(?i)AutoModelForCausalLM\.from_pretrained
(?i)pipeline\s*\(\s*["']text-generation
```

If any of these patterns are found, the notebook is NOT eligible for serverless and should remain on an ML runtime cluster.

---

## Migration Checklist for ML Notebooks

- [ ] Identify which ML packages are used (imports scan)
- [ ] Classify as serverless-eligible or ML-runtime-required
- [ ] For serverless-eligible: add required packages to requirements.txt
- [ ] Verify package versions are cp312-compatible
- [ ] Check for pandas 1.x → 2.x breaking changes
- [ ] Check for numpy deprecated aliases
- [ ] Check for scikit-learn API changes
- [ ] Check for XGBoost parameter removals
- [ ] Update MLlib `handleInvalid` settings for ANSI mode
- [ ] Update Feature Store → Feature Engineering client
- [ ] Verify model serialization compatibility (MLflow model format)
- [ ] Test model loading on target environment
