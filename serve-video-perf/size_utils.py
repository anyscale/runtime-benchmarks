import sys
import numpy as np
from typing import Any, Dict, List
import logging

logger = logging.getLogger("ray.serve")

def get_object_size_mb(obj: Any) -> float:
    """
    Get the approximate size of a Python object in MB.

    Args:
        obj: Any Python object

    Returns:
        Size in MB
    """
    size_bytes = sys.getsizeof(obj)

    # Handle numpy arrays more accurately
    if isinstance(obj, np.ndarray):
        size_bytes = obj.nbytes

    # Handle lists of numpy arrays (common for frames)
    elif isinstance(obj, list):
        size_bytes = sum(
            item.nbytes if isinstance(item, np.ndarray) else sys.getsizeof(item)
            for item in obj
        )

    # Handle dictionaries
    elif isinstance(obj, dict):
        size_bytes = sys.getsizeof(obj)
        for key, value in obj.items():
            size_bytes += get_object_size_mb(key) * 1024 * 1024  # Convert back to bytes
            size_bytes += get_object_size_mb(value) * 1024 * 1024

    return size_bytes / (1024 * 1024)  # Convert to MB


def log_data_transfer(source: str, destination: str, data: Any, description: str = ""):
    """
    Log the size of data being transferred between deployments.

    Args:
        source: Name of the source deployment
        destination: Name of the destination deployment
        data: The data being transferred
        description: Optional description of the data
    """
    size_mb = get_object_size_mb(data)

    msg = f"DATA TRANSFER: {source} -> {destination}"
    if description:
        msg += f" ({description})"
    msg += f" | Size: {size_mb:.2f} MB"

    logger.info(msg)
    return size_mb


def get_frames_size_breakdown(frames: List[np.ndarray]) -> Dict[str, Any]:
    """
    Get detailed size breakdown for a list of frames.

    Args:
        frames: List of numpy array frames

    Returns:
        Dictionary with size breakdown
    """
    if not frames:
        return {
            "frame_count": 0,
            "total_size_mb": 0,
            "avg_frame_size_mb": 0
        }

    frame_sizes = [frame.nbytes for frame in frames]
    total_size = sum(frame_sizes)

    return {
        "frame_count": len(frames),
        "total_size_mb": total_size / (1024 * 1024),
        "avg_frame_size_mb": (total_size / len(frames)) / (1024 * 1024),
        "min_frame_size_mb": min(frame_sizes) / (1024 * 1024),
        "max_frame_size_mb": max(frame_sizes) / (1024 * 1024),
        "frame_shape": frames[0].shape if frames else None,
        "frame_dtype": str(frames[0].dtype) if frames else None
    }


def log_frame_transfer(source: str, destination: str, frames: List[np.ndarray], chunk_id: int = None):
    """
    Log detailed information about frame data transfer.

    Args:
        source: Name of the source deployment
        destination: Name of the destination deployment
        frames: List of numpy array frames
        chunk_id: Optional chunk ID
    """
    breakdown = get_frames_size_breakdown(frames)

    chunk_info = f"Chunk {chunk_id} | " if chunk_id is not None else ""
    logger.info(
        f"FRAME TRANSFER: {source} -> {destination} | "
        f"{chunk_info}"
        f"{breakdown['frame_count']} frames | "
        f"Total: {breakdown['total_size_mb']:.2f} MB | "
        f"Avg: {breakdown['avg_frame_size_mb']:.3f} MB/frame | "
        f"Shape: {breakdown['frame_shape']} | "
        f"Dtype: {breakdown['frame_dtype']}"
    )

    return breakdown