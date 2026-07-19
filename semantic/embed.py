import logging
import os
from typing import List

from fastembed import TextEmbedding
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_NAME = "BAAI/bge-small-en-v1.5"
EMBED_DIM = 384

# Lazy-loaded local embedding model (ONNX via fastembed — no API key needed).
_model = None


def get_model() -> TextEmbedding:
    """Initialize and return the local embedding model (downloads ~130MB on first run)."""
    global _model
    if _model is None:
        logger.info(f"Loading local embedding model '{MODEL_NAME}'...")
        _model = TextEmbedding(model_name=MODEL_NAME)
    return _model


def _gpu_model():
    """Return (SentenceTransformer, device) if torch with a GPU backend is installed.

    Optional fast path for bulk index builds: on Apple Silicon (MPS) or CUDA the
    same bge model runs ~7x faster than CPU ONNX. torch is NOT a required
    dependency — without it we fall back to fastembed multiprocessing.
    """
    try:
        import torch
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None, None
    if torch.backends.mps.is_available():
        device = "mps"
    elif torch.cuda.is_available():
        device = "cuda"
    else:
        return None, None
    logger.info(f"Using GPU-accelerated embedding ({device}) via sentence-transformers.")
    return SentenceTransformer(MODEL_NAME, device=device), device


def embed_texts(texts: List[str], is_query: bool = False) -> List[List[float]]:
    """
    Generate embeddings for a list of texts using a local bge-small-en-v1.5 model.

    Args:
        texts: The list of string texts to embed.
        is_query: True for search queries — applies the BGE query instruction
                  prefix, which materially improves retrieval. False for documents.

    Returns:
        A list of 384-dimensional float list embeddings.
    """
    if not texts:
        return []

    processed = [t.replace("\n", " ") for t in texts]

    if is_query:
        model = get_model()
        vectors = model.query_embed(processed)
        return [v.tolist() for v in vectors]

    # Bulk document path. Prefer the GPU (MPS/CUDA) if torch is installed —
    # ~7x faster than CPU ONNX for long documents. normalize_embeddings=True
    # matches fastembed's L2-normalised output, so the two paths produce
    # interchangeable vectors (queries always go through fastembed).
    st_model, _device = _gpu_model()
    if st_model is not None:
        vecs = st_model.encode(
            processed, batch_size=64, normalize_embeddings=True,
            show_progress_bar=True,
        )
        return [v.tolist() for v in vecs]

    # CPU fallback: fastembed with multiprocessing across all cores.
    # embed() yields lazily — wrap in tqdm for live progress. Small
    # batch_size keeps the bar ticking in small steps.
    model = get_model()
    parallel = 0 if len(processed) > 50 else None
    vectors = model.embed(processed, batch_size=32, parallel=parallel)
    desc = f"Embedding {len(processed)} docs ({MODEL_NAME}, {os.cpu_count()} cores)"
    return [v.tolist() for v in tqdm(vectors, total=len(processed), desc=desc, unit="doc")]
