from typing import Dict, List, Any, Optional
from ray import serve
from ray.serve.handle import DeploymentHandle
import logging
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import ray
from video_chunker import VideoChunker
from model_inference import ObjectDetectionModel
from size_utils import log_frame_transfer, get_frames_size_breakdown
import time
import asyncio
logger = logging.getLogger("ray.serve")

chunk_size = 60

class VideoProcessingResponse(BaseModel):
    video_input: str
    chunk_count: int
    chunks: List[Dict[str, Any]]
    summary: Dict[str, Any]
    error: Optional[str] = None

@serve.deployment(
    num_replicas=24,  # Increased replicas for parallel processing
    ray_actor_options={"num_cpus": 1, "resources": {"decode_only": 0.01}},
    max_ongoing_requests=2
)
class VideoDecoder:    
    def __init__(self):
        self.chunker = VideoChunker()
        logger.info("VideoDecoder initialized")
    
    async def extract_frames_from_chunk(self, chunk_data: Dict[str, Any], fps: float = 1.0) -> Dict[str, Any]:
        chunk_id = chunk_data["chunk_id"]
        chunk_size_bytes = chunk_data["size_bytes"]
        logger.info(f"Extracting frames from memory chunk {chunk_id} of size {chunk_size_bytes}")
        frames = await self.chunker.extract_frames_from_memory_chunk(chunk_id, chunk_data, fps)

        # Log frame data size
        breakdown = log_frame_transfer("VideoDecoder", "VideoProcessingService", frames, chunk_id)

        return {
            "chunk_id": chunk_id,
            "frames": frames,
            "frame_count": len(frames),
            "size_mb": breakdown["total_size_mb"],
            "success": True
        }


@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 8, "num_gpus": 1},
    max_ongoing_requests=1
)
class ModelInference:    
    def __init__(self):
        self.model = ObjectDetectionModel()
        logger.info("ModelInference initialized")
    
    async def run_inference(self, frames: List[np.ndarray]) -> List[Dict[str, Any]]:
        logger.info(f"Running inference on {len(frames)} frames")
        predictions = self.model.predict(frames)
        logger.info(f"Inference completed for {len(frames)} frames")
        return predictions


@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 16, "resources": {"chunk_only": 0.01}},  # Force it to be on a separate node
    max_ongoing_requests=1
)
class VideoProcessingService:    
    def __init__(self, video_decoder: DeploymentHandle, model_inference: DeploymentHandle):
        self.video_decoder = video_decoder
        self.model_inference = model_inference
        self.chunker = VideoChunker()  # Chunking moved to API level
        logger.info("VideoProcessingService initialized")
    
    async def process_video(self, video_input: str, chunk_size: int = 60, combined_inference: bool = False) -> Dict[str, Any]:
        logger.info(f"Processing video in memory: {video_input}")
        start = time.time()
        # Step 1: Chunk video to memory (CPU-based)
        logger.info("Chunking video to memory...")
        chunk_data_list = self.chunker.chunk_video_to_memory(chunk_size, video_input)

        chunking_duration = time.time() - start
        logger.info(f"Created {len(chunk_data_list)} chunks in memory in {chunking_duration:.3f} seconds")
        
        chunking_end_time = time.time()
        # Step 2: Parallel frame extraction from memory chunks
        logger.info("Extracting frames from memory chunks in parallel...")
        frame_extraction_tasks = []
        for chunk_data in chunk_data_list:
            task = self.video_decoder.extract_frames_from_chunk.remote(chunk_data, fps=1.0)
            frame_extraction_tasks.append(task)
        
        # Wait for all frame extractions to complete
        chunk_results = []
        completed_results = await asyncio.gather(*frame_extraction_tasks)
        for result in completed_results:
            if result["success"]:
                chunk_results.append(result)
            else:
                logger.warning(f"Failed to extract frames from memory chunk: {result.get('error', 'Unknown error')}")
        del(frame_extraction_tasks)

        decode_duration = time.time() - chunking_end_time
        decode_end_time = time.time()
        logger.info(f"Extracted frames from {len(chunk_results)} memory chunks in {decode_duration:.3f} seconds")
        
        # Step 3: Run inference (GPU-based)
        results = {
            "video_input": video_input,
            "chunk_count": len(chunk_data_list),
            "chunks": [],
            "summary": {
                "total_frames_processed": 0,
                "top_predictions": []
            }
        }
        
        if combined_inference:
            # Combined inference: send all frames from all chunks together
            logger.info("Running combined inference on all frames from all chunks...")
            all_frames = []
            frame_to_chunk_mapping = []  # Track which chunk each frame belongs to
            
            for chunk_result in chunk_results:
                chunk_id = chunk_result["chunk_id"]
                frames = chunk_result["frames"]
                
                if not frames:
                    logger.warning(f"No frames in memory chunk {chunk_id}, skipping")
                    continue
                
                all_frames.extend(frames)
                # Map each frame to its original chunk
                frame_to_chunk_mapping.extend([chunk_id] * len(frames))
            
            logger.info(f"Running combined inference on {len(all_frames)} total frames from {len(chunk_results)} chunks")
            
            # Run inference on all frames at once
            all_predictions = await self.model_inference.run_inference.remote(all_frames)
            
            # Distribute predictions back to chunks
            frame_idx = 0
            for chunk_result in chunk_results:
                chunk_id = chunk_result["chunk_id"]
                frames = chunk_result["frames"]
                size_mb = chunk_result["size_mb"]
                
                if not frames:
                    continue
                
                # Get predictions for this chunk's frames
                chunk_predictions = all_predictions[frame_idx:frame_idx + len(frames)]
                frame_idx += len(frames)
                
                # Process results
                chunk_result_final = {
                    "chunk_id": chunk_id,
                    "frame_count": len(frames),
                    "size_mb": size_mb,
                    "predictions": chunk_predictions,
                    "top_predictions": [pred["top_prediction"] for pred in chunk_predictions] if chunk_predictions else []
                }
                
                results["chunks"].append(chunk_result_final)
                results["summary"]["total_frames_processed"] += len(frames)
                
                # Collect top predictions for summary
                if chunk_predictions:
                    for pred in chunk_predictions:
                        if pred["top_prediction"]["confidence"] > 0.5:  # Only high-confidence predictions
                            results["summary"]["top_predictions"].append({
                                "chunk": chunk_id,
                                "prediction": pred["top_prediction"]
                            })
        else:
            # Original chunk-by-chunk inference
            logger.info("Running chunk-by-chunk inference...")
            prediction_tasks = []
            # Process each chunk with frames
            for chunk_result in chunk_results:
                chunk_id = chunk_result["chunk_id"]
                frames = chunk_result["frames"]
                
                if not frames:
                    logger.warning(f"No frames in memory chunk {chunk_id}, skipping")
                    continue
                
                logger.info(f"Running inference on memory chunk {chunk_id+1}/{len(chunk_data_list)} with {len(frames)} frames")
                
                # Run inference on frames using GPU service
                task = self.model_inference.run_inference.remote(frames)
                prediction_tasks.append(task)
            
            predictions_list = await asyncio.gather(*prediction_tasks)
            for i, predictions in enumerate(predictions_list):
                frames = chunk_results[i]["frames"]
                chunk_id = chunk_results[i]["chunk_id"]
                size_mb = chunk_results[i]["size_mb"]
                # Process results
                chunk_result_final = {
                    "chunk_id": chunk_id,
                    "frame_count": len(frames),
                    "size_mb": size_mb,
                    "predictions": predictions,
                    "top_predictions": [pred["top_prediction"] for pred in predictions] if predictions else []
                }
                
                results["chunks"].append(chunk_result_final)
                results["summary"]["total_frames_processed"] += len(frames)
                
                # Collect top predictions for summary
                if predictions:
                    for pred in predictions:
                        if pred["top_prediction"]["confidence"] > 0.5:  # Only high-confidence predictions
                            results["summary"]["top_predictions"].append({
                                "chunk": chunk_id,
                                "prediction": pred["top_prediction"]
                            })
        
        inference_duration = time.time() - decode_end_time
        logger.info(f"Video processing completed. Processed {results['summary']['total_frames_processed']} frames in {inference_duration:.3f} seconds")
        return results


fastapi_app = FastAPI()

@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_cpus": 1},
    max_ongoing_requests=10
)
@serve.ingress(fastapi_app)
class VideoProcessingAPI:    
    def __init__(self, video_processor: DeploymentHandle):
        self.video_processor = video_processor    
        
    @fastapi_app.get("/process-video", response_model=VideoProcessingResponse)
    async def process_video(self, video_id: str, chunk_size: int = 60, combined_inference: bool = False):
        try:
            result = await self.video_processor.process_video.remote(
                video_id, chunk_size, combined_inference
            )
            
            if "error" in result:
                raise HTTPException(status_code=400, detail=result["error"])
            
            return VideoProcessingResponse(**result)
            
        except Exception as e:
            logger.error(f"Error processing video: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        
# Create the Ray Serve application
# Create separate deployments
video_decoder = VideoDecoder.bind()
model_inference = ModelInference.bind()
video_processor = VideoProcessingService.bind(video_decoder, model_inference)
api = VideoProcessingAPI.bind(video_processor)